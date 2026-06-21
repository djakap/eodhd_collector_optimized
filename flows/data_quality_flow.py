"""
Prefect Flow: Data Quality Checks for QuestDB

Four daily/weekly checks:
1. cleanup_metadata       — remove 330K duplicate rows, prevent future accumulation
2. validate_ohlc          — detect price anomalies (inverted OHLC, spikes, zero vol)
3. detect_new_symbols     — compare EODHD JK exchange list vs tracked metadata
4. monitor_partitions     — partition sizes, WAL backlog, disk usage trend
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional

import psycopg2

from prefect import flow, task, get_run_logger

from db.questdb_client import QuestDBClient
from config.db_config import (
    TABLE_STOCK_DATA,
    TABLE_STOCK_METADATA,
    QUESTDB_HOST,
    QUESTDB_PG_PORT,
    QUESTDB_USER,
    QUESTDB_PASSWORD,
    QUESTDB_DATABASE,
)

logger = logging.getLogger(__name__)


def _raw_conn():
    """Direct psycopg2 connection (bypasses pool — for DDL / admin queries)."""
    return psycopg2.connect(
        host=QUESTDB_HOST,
        port=QUESTDB_PG_PORT,
        user=QUESTDB_USER,
        password=QUESTDB_PASSWORD,
        database=QUESTDB_DATABASE,
    )


# ---------------------------------------------------------------------------
# Task 1 — Metadata cleanup
# ---------------------------------------------------------------------------

@task(name="Cleanup metadata duplicates", retries=1, retry_delay_seconds=30)
def cleanup_metadata_duplicates() -> Dict:
    log = get_run_logger()
    conn = _raw_conn()
    cur = conn.cursor()

    try:
        cur.execute(f"SELECT count() FROM {TABLE_STOCK_METADATA}")
        total_before = cur.fetchone()[0]

        cur.execute(
            f"SELECT count() FROM "
            f"(SELECT symbol, interval FROM {TABLE_STOCK_METADATA} GROUP BY symbol, interval)"
        )
        unique_count = cur.fetchone()[0]
        dup_count = total_before - unique_count

        log.info(f"Metadata rows before: {total_before:,}  unique keys: {unique_count:,}  duplicates: {dup_count:,}")

        if dup_count == 0:
            log.info("No duplicates found — skipping rebuild")
            return {"status": "ok", "rows_before": total_before, "rows_after": total_before, "removed": 0}

        # Step 1: fetch latest row per (symbol, interval) from current table
        cur.execute(f"""
            SELECT symbol, interval, last_updated, total_records, data_start, data_end, created_at
            FROM {TABLE_STOCK_METADATA}
            ORDER BY symbol, interval, last_updated DESC
        """)
        rows = cur.fetchall()

        seen = {}
        dedup_rows = []
        for row in rows:
            sym, ivl = row[0], row[1]
            key = (sym, ivl)
            if key not in seen:
                seen[key] = True
                dedup_rows.append(row)

        log.info(f"Fetched {len(dedup_rows)} unique rows — will rebuild table")

        # Step 2: drop all partitions to truncate the table
        cur.execute(f"SELECT name FROM table_partitions('{TABLE_STOCK_METADATA}') ORDER BY name")
        partitions = [r[0] for r in cur.fetchall()]

        if partitions:
            partition_list = ", ".join(f"'{p}'" for p in partitions)
            cur.execute(f"ALTER TABLE {TABLE_STOCK_METADATA} DROP PARTITION LIST {partition_list}")
            log.info(f"Dropped {len(partitions)} partitions")

        # Step 3: re-insert deduplicated rows
        now = datetime.now()
        insert_sql = f"""
            INSERT INTO {TABLE_STOCK_METADATA}
            (symbol, interval, last_updated, total_records, data_start, data_end, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        batch = [
            (r[0], r[1], r[2], r[3], r[4], r[5], r[6] or now)
            for r in dedup_rows
        ]
        from psycopg2.extras import execute_batch
        execute_batch(cur, insert_sql, batch, page_size=500)
        conn.commit()

        log.info(f"Re-inserted {len(batch):,} unique rows — cleanup complete")
        return {
            "status": "ok",
            "rows_before": total_before,
            "rows_after": len(batch),
            "removed": dup_count,
        }

    except Exception as e:
        conn.rollback()
        log.error(f"Metadata cleanup failed: {e}")
        raise
    finally:
        cur.close()
        conn.close()


# ---------------------------------------------------------------------------
# Task 2 — OHLC validation
# ---------------------------------------------------------------------------

@task(name="Validate OHLC data", retries=1, retry_delay_seconds=30)
def validate_ohlc_data(lookback_days: int = 7, spike_threshold: float = 0.5) -> Dict:
    log = get_run_logger()
    conn = _raw_conn()
    cur = conn.cursor()

    try:
        since = (datetime.now() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
        log.info(f"Checking OHLC anomalies since {since} (spike threshold: {spike_threshold*100:.0f}%)")

        # 1. Inverted high/low
        cur.execute(f"""
            SELECT symbol, interval, timestamp, open, high, low, close, volume
            FROM {TABLE_STOCK_DATA}
            WHERE timestamp >= '{since}'
              AND interval = 'd'
              AND high < low
            ORDER BY timestamp DESC
            LIMIT 200
        """)
        inverted = cur.fetchall()

        # 2. Open/close outside high-low range
        cur.execute(f"""
            SELECT symbol, interval, timestamp, open, high, low, close
            FROM {TABLE_STOCK_DATA}
            WHERE timestamp >= '{since}'
              AND interval = 'd'
              AND (open > high OR open < low OR close > high OR close < low)
              AND high > 0 AND low > 0
            ORDER BY timestamp DESC
            LIMIT 200
        """)
        ohlc_range_violations = cur.fetchall()

        # 3. Zero or negative prices (EOD only)
        cur.execute(f"""
            SELECT symbol, interval, timestamp, open, high, low, close
            FROM {TABLE_STOCK_DATA}
            WHERE timestamp >= '{since}'
              AND interval = 'd'
              AND (open <= 0 OR high <= 0 OR low <= 0 OR close <= 0)
            ORDER BY symbol, timestamp DESC
            LIMIT 200
        """)
        zero_prices = cur.fetchall()

        # 4. Zero volume on EOD rows (suspicious for active stocks)
        cur.execute(f"""
            SELECT count() FROM {TABLE_STOCK_DATA}
            WHERE timestamp >= '{since}'
              AND interval = 'd'
              AND volume = 0
        """)
        zero_vol_count = cur.fetchone()[0]

        # 5. Price spikes: close today vs close yesterday > threshold
        # Use a self-join approach via Python — fetch last 2 rows per symbol
        cur.execute(f"""
            SELECT symbol, timestamp, close
            FROM {TABLE_STOCK_DATA}
            WHERE timestamp >= '{since}'
              AND interval = 'd'
              AND close > 0
            ORDER BY symbol, timestamp DESC
        """)
        price_rows = cur.fetchall()

        # Group by symbol, find consecutive-day spikes
        from collections import defaultdict
        by_symbol = defaultdict(list)
        for sym, ts, close in price_rows:
            by_symbol[sym].append((ts, close))

        spikes = []
        for sym, prices in by_symbol.items():
            for i in range(len(prices) - 1):
                ts_new, c_new = prices[i]
                ts_old, c_old = prices[i + 1]
                if c_old > 0:
                    change = abs(c_new - c_old) / c_old
                    if change > spike_threshold:
                        spikes.append({
                            "symbol": sym,
                            "date": str(ts_new)[:10],
                            "prev_close": round(c_old, 2),
                            "close": round(c_new, 2),
                            "change_pct": round(change * 100, 1),
                        })

        # Summary
        total_issues = len(inverted) + len(ohlc_range_violations) + len(zero_prices) + len(spikes)
        log.info(f"OHLC check results (last {lookback_days} days):")
        log.info(f"  Inverted high/low    : {len(inverted)}")
        log.info(f"  OHLC range violations: {len(ohlc_range_violations)}")
        log.info(f"  Zero/neg prices      : {len(zero_prices)}")
        log.info(f"  Zero volume rows     : {zero_vol_count:,}")
        log.info(f"  Price spikes >{spike_threshold*100:.0f}%  : {len(spikes)}")

        if len(inverted) > 0:
            log.warning(f"INVERTED H/L — first 5: {[(r[0], str(r[2])[:10], r[4], r[5]) for r in inverted[:5]]}")
        if len(ohlc_range_violations) > 0:
            log.warning(f"OHLC range violations — first 5: {[(r[0], str(r[2])[:10]) for r in ohlc_range_violations[:5]]}")
        if len(zero_prices) > 0:
            log.warning(f"Zero prices — first 5: {[(r[0], str(r[2])[:10]) for r in zero_prices[:5]]}")
        if len(spikes) > 10:
            log.warning(f"Price spikes — first 10: {spikes[:10]}")
        elif spikes:
            log.warning(f"Price spikes: {spikes}")

        return {
            "status": "ok" if total_issues == 0 else "issues_found",
            "lookback_days": lookback_days,
            "inverted_hl": len(inverted),
            "ohlc_violations": len(ohlc_range_violations),
            "zero_prices": len(zero_prices),
            "zero_volume_rows": zero_vol_count,
            "price_spikes": len(spikes),
            "total_issues": total_issues,
            "spike_details": spikes[:20],
        }

    except Exception as e:
        log.error(f"OHLC validation failed: {e}")
        raise
    finally:
        cur.close()
        conn.close()


# ---------------------------------------------------------------------------
# Task 3 — Detect new exchange symbols
# ---------------------------------------------------------------------------

@task(name="Detect new exchange symbols", retries=2, retry_delay_seconds=60)
def detect_new_exchange_symbols(stocks_file: str = "config/syariah_stocks.txt") -> Dict:
    log = get_run_logger()

    # Load tracked symbols from file — file uses bare codes (e.g. BBCA), add .JK suffix
    tracked: set = set()
    if os.path.exists(stocks_file):
        with open(stocks_file) as f:
            for line in f:
                sym = line.strip()
                if sym and not sym.startswith('#'):
                    tracked.add(sym if sym.endswith('.JK') else f"{sym}.JK")
    log.info(f"Tracked symbols from {stocks_file}: {len(tracked)}")

    # Load symbols we have metadata for
    conn = _raw_conn()
    cur = conn.cursor()
    try:
        cur.execute(f"""
            SELECT symbol FROM {TABLE_STOCK_METADATA}
            WHERE interval = 'd'
            ORDER BY symbol
        """)
        in_db = {r[0] for r in cur.fetchall()}
    finally:
        cur.close()
        conn.close()
    log.info(f"Symbols with EOD metadata in DB: {len(in_db)}")

    # Fetch EODHD exchange list
    eodhd_symbols: set = set()
    try:
        import httpx
        api_key = os.getenv("EODHD_API_KEY", "")
        if not api_key:
            log.warning("EODHD_API_KEY not set — skipping exchange list fetch")
        else:
            resp = httpx.get(
                "https://eodhistoricaldata.com/api/exchange-symbol-list/JK",
                params={"api_token": api_key, "fmt": "json"},
                timeout=30,
            )
            if resp.status_code == 200:
                data = resp.json()
                for item in data:
                    code = item.get("Code", "")
                    if code:
                        eodhd_symbols.add(f"{code}.JK")
                log.info(f"EODHD JK exchange symbols (Common Stock): {len(eodhd_symbols)}")
            else:
                log.warning(f"EODHD exchange list returned HTTP {resp.status_code}")
    except Exception as e:
        log.warning(f"Could not fetch EODHD exchange list: {e}")

    # Analysis
    not_tracked = sorted(eodhd_symbols - tracked) if eodhd_symbols else []
    not_in_db = sorted(tracked - in_db)
    eodhd_not_known = sorted(eodhd_symbols - in_db) if eodhd_symbols else []

    log.info(f"Symbols on EODHD JK not in our tracking file: {len(not_tracked)}")
    log.info(f"Tracked symbols missing from DB metadata    : {len(not_in_db)}")
    if not_in_db:
        log.warning(f"Missing from DB: {not_in_db[:20]}")
    if not_tracked and len(not_tracked) <= 50:
        log.info(f"EODHD symbols not tracked: {not_tracked}")
    elif not_tracked:
        log.info(f"EODHD symbols not tracked (first 50): {not_tracked[:50]}")

    return {
        "status": "ok",
        "tracked_count": len(tracked),
        "in_db_count": len(in_db),
        "eodhd_exchange_count": len(eodhd_symbols),
        "not_in_tracking_file": len(not_tracked),
        "tracked_missing_from_db": not_in_db,
        "eodhd_new_symbols_sample": not_tracked[:30],
    }


# ---------------------------------------------------------------------------
# Task 4 — Partition monitor
# ---------------------------------------------------------------------------

@task(name="Monitor QuestDB partitions", retries=1, retry_delay_seconds=30)
def monitor_questdb_partitions(warn_partition_mb: int = 200) -> Dict:
    log = get_run_logger()
    conn = _raw_conn()
    cur = conn.cursor()

    try:
        # Main data table partitions
        cur.execute(f"""
            SELECT name, numRows, diskSize
            FROM table_partitions('{TABLE_STOCK_DATA}')
            ORDER BY name
        """)
        partitions = cur.fetchall()

        total_rows = sum(r[1] for r in partitions)
        total_size_mb = sum(r[2] for r in partitions) / 1024 / 1024

        # WAL (unmerged) segments
        wal_segs = [(r[0], r[1], r[2]) for r in partitions if 'T' in r[0]]
        normal_parts = [(r[0], r[1], r[2]) for r in partitions if 'T' not in r[0]]

        wal_rows = sum(r[1] for r in wal_segs)
        wal_size_mb = sum(r[2] for r in wal_segs) / 1024 / 1024

        # Largest partitions
        top5 = sorted(normal_parts, key=lambda r: r[2], reverse=True)[:5]

        # Partitions over the warning threshold
        large = [(r[0], round(r[2]/1024/1024, 1)) for r in normal_parts if r[2]/1024/1024 > warn_partition_mb]

        # Recent growth: last 6 months vs 6 months before that
        from datetime import date
        six_months_ago = (date.today().replace(day=1) - timedelta(days=180)).strftime("%Y-%m")
        year_ago = (date.today().replace(day=1) - timedelta(days=365)).strftime("%Y-%m")

        recent_size = sum(r[2] for r in normal_parts if r[0] >= six_months_ago) / 1024 / 1024
        prev_size = sum(
            r[2] for r in normal_parts if year_ago <= r[0] < six_months_ago
        ) / 1024 / 1024

        # Metadata table
        cur.execute(f"""
            SELECT count() FROM {TABLE_STOCK_METADATA}
        """)
        meta_rows = cur.fetchone()[0]

        log.info(f"=== QuestDB Partition Monitor ===")
        log.info(f"Main table: {len(normal_parts)} partitions | {total_rows:,} rows | {total_size_mb:.0f} MB")
        log.info(f"WAL backlog: {len(wal_segs)} segments | {wal_rows:,} rows | {wal_size_mb:.1f} MB")
        log.info(f"Last 6mo size : {recent_size:.0f} MB  |  Prior 6mo: {prev_size:.0f} MB")
        log.info(f"Metadata table: {meta_rows:,} rows")
        log.info(f"Top 5 largest partitions:")
        for name, rows, size in top5:
            log.info(f"  {name}  {rows:>10,} rows  {size/1024/1024:.1f} MB")

        if large:
            log.warning(f"Partitions over {warn_partition_mb}MB: {large}")
        if wal_rows > 100_000:
            log.warning(f"WAL backlog is large: {wal_rows:,} unmerged rows in {len(wal_segs)} segments")
        if meta_rows > 50_000:
            log.warning(f"Metadata table has {meta_rows:,} rows — consider running cleanup_metadata_duplicates")

        return {
            "status": "ok",
            "partitions": len(normal_parts),
            "total_rows": total_rows,
            "total_size_mb": round(total_size_mb, 1),
            "wal_segments": len(wal_segs),
            "wal_rows": wal_rows,
            "wal_size_mb": round(wal_size_mb, 1),
            "recent_6mo_mb": round(recent_size, 1),
            "prev_6mo_mb": round(prev_size, 1),
            "large_partitions": large,
            "metadata_rows": meta_rows,
        }

    except Exception as e:
        log.error(f"Partition monitor failed: {e}")
        raise
    finally:
        cur.close()
        conn.close()


# ---------------------------------------------------------------------------
# Main flow
# ---------------------------------------------------------------------------

@flow(name="Data Quality Check", log_prints=True, timeout_seconds=3600)
def data_quality_flow(
    stocks_file: str = "config/syariah_stocks.txt",
    lookback_days: int = 7,
    spike_threshold: float = 0.5,
    warn_partition_mb: int = 200,
    run_cleanup: bool = True,
    run_ohlc: bool = True,
    run_symbols: bool = True,
    run_partitions: bool = True,
):
    """
    Weekly data quality check for the EODHD QuestDB pipeline.

    Runs four independent checks and logs a summary. No data is modified
    except when run_cleanup=True (metadata table deduplication).
    """
    log = get_run_logger()
    results = {}

    if run_cleanup:
        log.info("--- [1/4] Metadata table cleanup ---")
        results["metadata"] = cleanup_metadata_duplicates()

    if run_ohlc:
        log.info("--- [2/4] OHLC validation ---")
        results["ohlc"] = validate_ohlc_data(
            lookback_days=lookback_days,
            spike_threshold=spike_threshold,
        )

    if run_symbols:
        log.info("--- [3/4] New symbol detection ---")
        results["symbols"] = detect_new_exchange_symbols(stocks_file=stocks_file)

    if run_partitions:
        log.info("--- [4/4] Partition monitor ---")
        results["partitions"] = monitor_questdb_partitions(warn_partition_mb=warn_partition_mb)

    # Final summary
    log.info("=== Data Quality Summary ===")
    if "metadata" in results:
        m = results["metadata"]
        log.info(f"  Metadata: removed {m.get('removed', 0):,} duplicates → {m.get('rows_after', 0):,} rows")
    if "ohlc" in results:
        o = results["ohlc"]
        log.info(f"  OHLC: {o.get('total_issues', 0)} issues "
                 f"(inv={o.get('inverted_hl',0)}, spikes={o.get('price_spikes',0)}, "
                 f"zero_vol={o.get('zero_volume_rows',0)})")
    if "symbols" in results:
        s = results["symbols"]
        log.info(f"  Symbols: tracked={s.get('tracked_count',0)}, in_db={s.get('in_db_count',0)}, "
                 f"eodhd_exchange={s.get('eodhd_exchange_count',0)}")
    if "partitions" in results:
        p = results["partitions"]
        log.info(f"  Partitions: {p.get('total_rows',0):,} rows | {p.get('total_size_mb',0):.0f} MB | "
                 f"WAL backlog {p.get('wal_rows',0):,} rows")

    QuestDBClient.close_pool()
    return results


if __name__ == "__main__":
    data_quality_flow()
