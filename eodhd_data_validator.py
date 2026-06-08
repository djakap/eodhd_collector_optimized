"""
EODHD Data Quality Validator
4-stage data observability audit for QuestDB OHLCV data.

Stage 1: EOD duplicate detection   — per-symbol GROUP BY in a narrow time window
Stage 2: EOD staleness & gap       — via eodhd_stock_metadata (pre-aggregated)
Stage 3: EOD API consistency       — QuestDB vs EODHD EOD API (row-level)
Stage 4: Intraday data quality     — staleness, cross-interval consistency,
                                     bars-per-day completeness, duplicate check,
                                     and spot-check vs EODHD intraday API

Design notes:
- All analytical queries use QuestDB's HTTP REST API (port 9001 on host) which
  is more stable than psycopg2 for heavy aggregates on the 19 M-row stock table.
- Per-symbol, date-windowed queries prevent full-table-scan OOM crashes.
- psycopg2 is kept only for simple range fetches in Stage 3 & 4e.
"""

import os
import random
import sys
import time
from datetime import date, datetime, timedelta
from typing import Optional

import psycopg2
import requests

# Load .env so os.getenv picks up project variables
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
except ImportError:
    pass

# ── Config — all values come from .env / environment, never hardcoded ────────
QUESTDB_HOST      = os.getenv("QUESTDB_HOST",      "localhost")
QUESTDB_PG_PORT   = int(os.getenv("QUESTDB_PG_PORT",   "8812"))
QUESTDB_HTTP_PORT = int(os.getenv("QUESTDB_HTTP_PORT",  "9001"))  # host-mapped from 9000
QUESTDB_USER      = os.getenv("QUESTDB_USER",      "admin")
QUESTDB_PASSWORD  = os.getenv("QUESTDB_PASSWORD",  "quest")
QUESTDB_DB        = os.getenv("QUESTDB_DATABASE",  "qdb")

EODHD_API_KEY  = os.getenv("EODHD_API_KEY", "")
EODHD_BASE_URL = os.getenv("EODHD_BASE_URL", "https://eodhistoricaldata.com/api")

TABLE_STOCK     = "eodhd_stock_data"
TABLE_META      = "eodhd_stock_metadata"
EOD_INTERVAL    = "d"

# Stage 1: how many symbols to sample for EOD duplicate check
DUP_SAMPLE_SIZE = 10
# Stage 1: look back N days from each symbol's data_end date
DUP_WINDOW_DAYS = 365
# Stage 3: compare this many calendar days against EOD API
COMPARE_DAYS    = 60

# Stage 4: intraday intervals to audit
INTRADAY_INTERVALS = ["5m", "15m", "30m", "1h"]

# Expected trading bars per day for IDX (Jakarta):
# Session 1: 09:00-11:30 WIB (150 min), Session 2: 13:30-15:55 WIB (145 min)
# Total ≈ 295 min/day. Values confirmed against actual DB data.
EXPECTED_BARS_DAY = {"5m": 59, "15m": 20, "30m": 10, "1h": 6}

# Stage 4b/4d: symbols to sample for intraday duplicate + bars-per-day checks
INTRADAY_SAMPLE  = 5
# Stage 4d: window for bars-per-day analysis (trading days, from data_end)
INTRADAY_BPDAY_WINDOW = 30
# Stage 4e: intraday API spot-check — look back N days from data_end
INTRADAY_API_WINDOW = 5

# ── ANSI colour helpers ───────────────────────────────────────────────────────
GREEN  = "\033[92m"; YELLOW = "\033[93m"; RED = "\033[91m"
BOLD   = "\033[1m";  RESET  = "\033[0m"

def passed(msg):  print(f"  {GREEN}[PASSED]{RESET}  {msg}")
def warning(msg): print(f"  {YELLOW}[WARNING]{RESET} {msg}")
def failed(msg):  print(f"  {RED}[FAILED]{RESET}  {msg}")
def info(msg):    print(f"           {msg}")
def header(msg):  print(f"\n{BOLD}{msg}{RESET}")
def divider():    print("─" * 70)


# ── QuestDB HTTP REST helper ──────────────────────────────────────────────────
HTTP_BASE = f"http://{QUESTDB_HOST}:{QUESTDB_HTTP_PORT}"

def http_query(sql: str, pause: float = 0.5) -> tuple[Optional[list], Optional[list]]:
    """
    Execute SQL via QuestDB HTTP /exec endpoint.
    Returns (rows, col_names) or (None, error_message).
    """
    time.sleep(pause)
    try:
        r = requests.get(f"{HTTP_BASE}/exec", params={"query": sql}, timeout=45)
        if not r.ok:
            return None, f"HTTP {r.status_code}: {r.text[:200]}"
        d = r.json()
        if "error" in d:
            return None, d["error"]
        cols = [c["name"] for c in d.get("columns", [])]
        return d.get("dataset", []), cols
    except Exception as e:
        return None, str(e)


# ── psycopg2 helper (for single-symbol range queries in Stage 3) ──────────────
def pg_query(sql: str) -> Optional[list]:
    conn = psycopg2.connect(
        host=QUESTDB_HOST, port=QUESTDB_PG_PORT,
        user=QUESTDB_USER, password=QUESTDB_PASSWORD, database=QUESTDB_DB,
    )
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetchall()
    finally:
        conn.close()


# ── EODHD API helper ──────────────────────────────────────────────────────────
def fetch_eod_api(symbol: str, from_date: str, to_date: str) -> Optional[list]:
    url = f"{EODHD_BASE_URL}/eod/{symbol}"
    try:
        r = requests.get(url, params={
            "api_token": EODHD_API_KEY, "fmt": "json",
            "period": "d", "from": from_date, "to": to_date,
        }, timeout=25)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        info(f"API request failed for {symbol}: {e}")
        return None


# ════════════════════════════════════════════════════════════════════════════
# STAGE 1 — Duplicate Detection (per-symbol, windowed GROUP BY)
# ════════════════════════════════════════════════════════════════════════════
def stage1_duplicates() -> bool:
    header("STAGE 1 ─ Duplicate Detection  (symbol + timestamp, sampled approach)")
    divider()

    # Get a sample of symbols from metadata (those with most records = most likely to expose dups)
    rows, err = http_query(f"""
        SELECT symbol, max(data_end) AS last_date, max(total_records) AS recs
        FROM   {TABLE_META}
        WHERE  interval = '{EOD_INTERVAL}'
        GROUP  BY symbol
        ORDER  BY recs DESC
        LIMIT  200
    """)
    if rows is None:
        failed(f"Could not fetch symbol list: {err}")
        return False

    if not rows:
        failed("No EOD symbols found in metadata table.")
        return False

    # Pick a random sample weighted towards larger datasets
    sample = random.sample(rows, min(DUP_SAMPLE_SIZE, len(rows)))
    info(f"Sampling {len(sample)} symbols (out of {len(rows)} in metadata)")

    total_dup_pairs  = 0
    total_extra_rows = 0
    dup_symbols      = []

    for sym, last_date_str, _recs in sample:
        # Compute window start
        try:
            last_dt = datetime.fromisoformat(last_date_str.replace("Z", "+00:00"))
            win_start = (last_dt - timedelta(days=DUP_WINDOW_DAYS)).strftime("%Y-%m-%dT00:00:00Z")
            win_end   = last_dt.strftime("%Y-%m-%dT23:59:59Z")
        except Exception:
            win_start = "2000-01-01T00:00:00Z"
            win_end   = "2030-01-01T00:00:00Z"

        dup_sql = f"""
            SELECT timestamp, cnt
            FROM (
                SELECT timestamp, count(*) AS cnt
                FROM   {TABLE_STOCK}
                WHERE  symbol   = '{sym}'
                AND    interval = '{EOD_INTERVAL}'
                AND    timestamp >= '{win_start}'
                AND    timestamp <= '{win_end}'
                GROUP  BY timestamp
            )
            WHERE cnt > 1
            ORDER BY cnt DESC
            LIMIT  10
        """
        dup_rows, dup_err = http_query(dup_sql, pause=0.3)

        if dup_rows is None:
            warning(f"  Could not check {sym}: {dup_err}")
            continue

        if dup_rows:
            pairs      = len(dup_rows)
            extra      = sum(r[1] - 1 for r in dup_rows)
            total_dup_pairs  += pairs
            total_extra_rows += extra
            dup_symbols.append((sym, pairs, extra, dup_rows))

    if not dup_symbols:
        passed(f"No duplicate (symbol, timestamp) pairs found in {len(sample)} sampled symbols.")
        return True

    failed(f"Duplicates detected in {len(dup_symbols)}/{len(sample)} sampled symbols "
           f"({total_dup_pairs} duplicate pairs, {total_extra_rows} extra rows).")

    for sym, pairs, extra, rows in dup_symbols:
        info(f"\n  Symbol {BOLD}{sym}{RESET}  — {pairs} duplicate dates, {extra} extra rows")
        info(f"    {'Timestamp':<30} {'Count':>6}")
        info(f"    " + "-" * 40)
        for ts, cnt in rows[:5]:
            info(f"    {str(ts):<30} {cnt:>6}")

    return False


# ════════════════════════════════════════════════════════════════════════════
# STAGE 2 — Staleness & Gap Analysis (via metadata table)
# ════════════════════════════════════════════════════════════════════════════
def stage2_missing() -> bool:
    header("STAGE 2 ─ Staleness & Missing-Data Analysis  (via eodhd_stock_metadata)")
    divider()
    stage_ok = True

    # Latest data_end per symbol (take the most recent metadata record per symbol)
    rows, err = http_query(f"""
        SELECT symbol, max(data_end) AS last_date,
               max(total_records)    AS row_count,
               max(data_start)       AS first_date
        FROM   {TABLE_META}
        WHERE  interval = '{EOD_INTERVAL}'
        GROUP  BY symbol
        ORDER  BY last_date ASC
    """)

    if rows is None:
        failed(f"Query error: {err}")
        return False
    if not rows:
        failed("No EOD metadata found.")
        return False

    sym_count = len(rows)
    info(f"Total symbols with EOD metadata: {sym_count}")

    # ── 2a: Staleness ─────────────────────────────────────────────────────
    dates = [r[1] for r in rows if r[1]]
    dates_parsed = []
    for d in dates:
        try:
            dates_parsed.append(datetime.fromisoformat(d.replace("Z", "+00:00")))
        except Exception:
            pass

    if not dates_parsed:
        failed("Could not parse any data_end timestamps.")
        return False

    dates_parsed.sort()
    majority = dates_parsed[len(dates_parsed) // 2]  # median
    most_recent = dates_parsed[-1]

    info(f"Most recent data point (any symbol) : {most_recent.date()}")
    info(f"Median latest date across symbols    : {majority.date()}")
    info(f"Days since most recent update        : "
         f"{(date.today() - most_recent.date()).days} days")

    STALE_THRESH = 7  # days behind median to flag as stale
    stale = []
    for sym, last_date, recs, first_date in rows:
        try:
            ld = datetime.fromisoformat(last_date.replace("Z", "+00:00"))
            lag = (majority - ld).days
            if lag > STALE_THRESH:
                stale.append((sym, ld.date(), lag, recs or 0))
        except Exception:
            pass

    if stale:
        warning(f"{len(stale)} symbols are stale (>{STALE_THRESH} days behind median date).")
        stage_ok = False
        info(f"\n  {'Symbol':<20} {'Last Date':<14} {'Lag(d)':>7} {'Rows':>8}")
        info("  " + "-" * 54)
        for sym, ld, lag, recs in sorted(stale, key=lambda x: -x[2])[:15]:
            info(f"  {sym:<20} {str(ld):<14} {lag:>7} {recs:>8}")
        if len(stale) > 15:
            info(f"  … and {len(stale) - 15} more stale symbols")
    else:
        passed("No stale symbols — all symbols have data within the staleness threshold.")

    # ── 2b: Gap / coverage analysis ───────────────────────────────────────
    row_counts = sorted([r[2] for r in rows if r[2] and r[2] > 0], reverse=True)
    max_rows = row_counts[0]
    min_rows = row_counts[-1]
    avg_rows = sum(row_counts) / len(row_counts)
    median_rows = row_counts[len(row_counts) // 2]

    info(f"\nRow-count distribution (by symbol):")
    info(f"  Max: {max_rows:>8,}   Min: {min_rows:>8,}")
    info(f"  Avg: {avg_rows:>8,.0f}   Median: {median_rows:>8,}")

    GAP_PCT = 0.20  # flag symbols with < 80% of max rows
    gap_symbols = [
        (r[0], r[2], round((max_rows - r[2]) / max_rows * 100, 1))
        for r in rows
        if r[2] and r[2] < max_rows * (1 - GAP_PCT)
    ]

    if gap_symbols:
        warning(f"{len(gap_symbols)} symbols have >{GAP_PCT*100:.0f}% fewer rows than the leader.")
        stage_ok = False
        info(f"\n  {'Symbol':<20} {'Rows':>8} {'Gap%':>8}")
        info("  " + "-" * 40)
        for sym, cnt, pct in sorted(gap_symbols, key=lambda x: -x[2])[:15]:
            info(f"  {sym:<20} {cnt:>8,} {pct:>7.1f}%")
        if len(gap_symbols) > 15:
            info(f"  … and {len(gap_symbols) - 15} more")
    else:
        passed("No significant row-count gaps detected across symbols.")

    # ── 2c: Overall data freshness summary ───────────────────────────────
    fresh_pct = (1 - len(stale) / sym_count) * 100
    info(f"\nData freshness summary: {sym_count - len(stale)}/{sym_count} symbols current "
         f"({fresh_pct:.1f}%)")

    if stage_ok:
        passed("Stage 2 overall: data coverage is healthy.")
    return stage_ok


# ════════════════════════════════════════════════════════════════════════════
# STAGE 3 — API vs QuestDB Consistency (row-level OHLCV)
# ════════════════════════════════════════════════════════════════════════════
def stage3_consistency() -> bool:
    header("STAGE 3 ─ API vs QuestDB Consistency  (row-level OHLCV comparison)")
    divider()
    stage_ok = True

    # Get a pool of symbols with their latest date
    rows, err = http_query(f"""
        SELECT symbol, max(data_end) AS last_date
        FROM   {TABLE_META}
        WHERE  interval = '{EOD_INTERVAL}'
        GROUP  BY symbol
        ORDER  BY last_date DESC
        LIMIT  100
    """)
    if rows is None or not rows:
        failed(f"Could not fetch symbols for Stage 3: {err}")
        return False

    # Pick 3 at random from the top-100 (most recently updated)
    sample = random.sample(rows, min(3, len(rows)))
    info(f"Randomly selected symbols: {', '.join(r[0] for r in sample)}")

    for sym, last_date_str in sample:
        info(f"\n  {'─'*62}")
        info(f"  Symbol: {BOLD}{sym}{RESET}")

        # Determine compare window
        try:
            last_dt   = datetime.fromisoformat(last_date_str.replace("Z", "+00:00"))
            to_date   = last_dt.date()
            from_date = to_date - timedelta(days=COMPARE_DAYS)
        except Exception:
            to_date   = date.today()
            from_date = to_date - timedelta(days=COMPARE_DAYS)

        from_str = from_date.strftime("%Y-%m-%d")
        to_str   = to_date.strftime("%Y-%m-%d")
        info(f"  Window: {from_str} → {to_str}  ({COMPARE_DAYS} calendar days)")

        # ── Fetch from QuestDB ───────────────────────────────────────────
        db_sql = (
            f"SELECT timestamp, open, high, low, close, volume "
            f"FROM {TABLE_STOCK} "
            f"WHERE symbol='{sym}' AND interval='{EOD_INTERVAL}' "
            f"AND timestamp >= '{from_str}T00:00:00Z' "
            f"AND timestamp <= '{to_str}T23:59:59Z' "
            f"ORDER BY timestamp LIMIT 500"
        )
        try:
            db_rows = pg_query(db_sql)
        except Exception as e:
            warning(f"  DB query failed for {sym}: {e}")
            continue

        db_map = {}
        for ts, o, h, l, c, v in (db_rows or []):
            d = ts.strftime("%Y-%m-%d") if hasattr(ts, "strftime") else str(ts)[:10]
            db_map[d] = {"open": float(o or 0), "high": float(h or 0),
                         "low": float(l or 0),  "close": float(c or 0),
                         "volume": int(v or 0)}

        # ── Fetch from EODHD API ─────────────────────────────────────────
        time.sleep(0.2)
        api_data = fetch_eod_api(sym, from_str, to_str)
        if api_data is None:
            warning(f"  API call failed — skipping {sym}.")
            continue

        api_map = {}
        for row in api_data:
            d = row.get("date", "")
            api_map[d] = {"open": float(row.get("open",   0) or 0),
                          "high": float(row.get("high",   0) or 0),
                          "low":  float(row.get("low",    0) or 0),
                          "close":float(row.get("close",  0) or 0),
                          "volume": int(row.get("volume", 0) or 0)}

        info(f"  DB rows : {len(db_map)}   |   API rows: {len(api_map)}")

        # ── Compare ──────────────────────────────────────────────────────
        all_dates   = sorted(set(api_map) | set(db_map))
        only_in_api = [d for d in all_dates if d in api_map and d not in db_map]
        only_in_db  = [d for d in all_dates if d in db_map and d not in api_map]

        PRICE_TOL = 0.01
        discrepancies = []
        for d in all_dates:
            if d not in api_map or d not in db_map:
                continue
            a, b = api_map[d], db_map[d]
            diffs = []
            for field in ("open", "high", "low", "close"):
                if abs(a[field] - b[field]) > PRICE_TOL:
                    diffs.append(f"{field}: API={a[field]:.4f} DB={b[field]:.4f}")
            if a["volume"] != b["volume"]:
                diffs.append(f"volume: API={a['volume']:,} DB={b['volume']:,}")
            if diffs:
                discrepancies.append((d, diffs))

        sym_ok = True

        if only_in_api:
            sym_ok = False
            stage_ok = False
            warning(f"  {len(only_in_api)} dates in API but missing from DB:")
            for d in only_in_api[:5]:
                info(f"    {d}  open={api_map[d]['open']}  close={api_map[d]['close']}")
            if len(only_in_api) > 5:
                info(f"    … and {len(only_in_api)-5} more")

        if only_in_db:
            info(f"  {len(only_in_db)} dates in DB but not in API "
                 f"(may be delisted / adjusted)  — not flagged as error")

        if discrepancies:
            sym_ok = False
            stage_ok = False
            warning(f"  {len(discrepancies)} dates with OHLCV discrepancies:")
            for d, diffs in discrepancies[:5]:
                info(f"    {d}: {'; '.join(diffs)}")
            if len(discrepancies) > 5:
                info(f"    … and {len(discrepancies)-5} more")

        if sym_ok:
            passed(f"  {sym}: DB matches API perfectly within the comparison window.")
        else:
            failed(f"  {sym}: discrepancies found (see above).")

    return stage_ok


# ── EODHD intraday API helper ─────────────────────────────────────────────────
def fetch_intraday_api(symbol: str, interval: str,
                       from_ts: int, to_ts: int) -> Optional[list]:
    url = f"{EODHD_BASE_URL}/intraday/{symbol}"
    try:
        r = requests.get(url, params={
            "api_token": EODHD_API_KEY, "fmt": "json",
            "interval": interval, "from": from_ts, "to": to_ts,
        }, timeout=25)
        if r.status_code == 403:
            return "FORBIDDEN"
        r.raise_for_status()
        return r.json()
    except Exception as e:
        info(f"Intraday API error ({symbol} {interval}): {e}")
        return None


# ════════════════════════════════════════════════════════════════════════════
# STAGE 4 — Intraday Data Quality
# ════════════════════════════════════════════════════════════════════════════
def stage4_intraday() -> bool:
    header("STAGE 4 ─ Intraday Data Quality  (5m / 15m / 30m / 1h)")
    divider()
    stage_ok = True

    # ── 4a: Per-interval staleness overview ──────────────────────────────────
    info(f"{BOLD}4a — Per-interval staleness overview (from metadata){RESET}")
    info("")

    interval_meta: dict[str, dict] = {}
    for iv in INTRADAY_INTERVALS:
        rows, err = http_query(f"""
            SELECT symbol, max(data_end) AS last_ts, max(total_records) AS rows
            FROM   {TABLE_META}
            WHERE  interval = '{iv}'
            GROUP  BY symbol
            ORDER  BY last_ts DESC
        """, pause=0.6)

        if rows is None:
            warning(f"  [{iv}] metadata query failed: {err}")
            continue

        if not rows:
            warning(f"  [{iv}] No metadata records found.")
            continue

        ts_list = []
        for sym, ts_str, recs in rows:
            try:
                ts_list.append(datetime.fromisoformat(ts_str.replace("Z", "+00:00")))
            except Exception:
                pass

        ts_list.sort()
        latest     = ts_list[-1]  if ts_list else None
        median_ts  = ts_list[len(ts_list)//2] if ts_list else None
        sym_count  = len(rows)

        # Count stale symbols (>7 hours behind median for intraday)
        STALE_H = 24  # hours — one full trading day behind median
        stale_cnt = sum(
            1 for ts in ts_list
            if median_ts and (median_ts - ts).total_seconds() > STALE_H * 3600
        )

        days_since = (datetime.now().astimezone() - latest).days if latest else "N/A"

        status_ok = stale_cnt == 0 and (isinstance(days_since, int) and days_since < 7)
        tag = f"{GREEN}OK{RESET}" if status_ok else f"{YELLOW}WARN{RESET}"

        info(f"  [{iv}]  symbols={sym_count:>4}  "
             f"latest={latest.strftime('%Y-%m-%d %H:%M') if latest else 'N/A':>17}  "
             f"stale={stale_cnt:>4}  "
             f"days_since_update={days_since}  [{tag}]")

        if stale_cnt > 0:
            stage_ok = False

        interval_meta[iv] = {"rows": rows, "latest": latest,
                              "median": median_ts, "sym_count": sym_count}

    # Global freshness note
    info("")
    if interval_meta:
        all_latest = [m["latest"] for m in interval_meta.values() if m.get("latest")]
        if all_latest:
            global_latest = max(all_latest)
            global_lag = (datetime.now().astimezone() - global_latest).days
            if global_lag > 7:
                failed(f"All intraday intervals are stale — "
                       f"last update was {global_lag} days ago "
                       f"({global_latest.strftime('%Y-%m-%d %H:%M')} UTC).")
                stage_ok = False
            else:
                passed(f"Intraday data is current (last update {global_lag} day(s) ago).")

    # ── 4b: Cross-interval symbol consistency ────────────────────────────────
    info("")
    info(f"{BOLD}4b — Cross-interval symbol consistency{RESET}")
    info("")

    sym_sets: dict[str, set] = {}
    for iv, meta in interval_meta.items():
        sym_sets[iv] = {r[0] for r in meta["rows"]}

    if len(sym_sets) == len(INTRADAY_INTERVALS):
        all_syms = set.union(*sym_sets.values())
        for iv in INTRADAY_INTERVALS:
            missing = all_syms - sym_sets.get(iv, set())
            extra   = sym_sets.get(iv, set()) - all_syms  # always empty here
            if missing:
                warning(f"  [{iv}] {len(missing)} symbols present in other intervals "
                        f"but NOT in this one: "
                        f"{', '.join(sorted(missing)[:5])}"
                        f"{'…' if len(missing) > 5 else ''}")
                stage_ok = False
            else:
                passed(f"  [{iv}] All {len(sym_sets[iv])} symbols present — consistent.")
    else:
        warning("  Could not compare intervals (some metadata queries failed).")

    # ── 4c: Bars-per-day completeness (per interval, sampled symbols) ────────
    info("")
    info(f"{BOLD}4c — Trading-session completeness  (bars/day, sampled){RESET}")
    info("")

    for iv in INTRADAY_INTERVALS:
        meta = interval_meta.get(iv)
        if not meta or not meta["rows"]:
            continue

        expected = EXPECTED_BARS_DAY[iv]
        # sample symbols with most rows (better signal)
        pool   = sorted(meta["rows"], key=lambda r: -(r[2] or 0))
        sample = pool[:INTRADAY_SAMPLE]

        low_symbols = []
        for sym, last_ts_str, total_recs in sample:
            # Compute window: last INTRADAY_BPDAY_WINDOW calendar days from data_end
            try:
                last_dt  = datetime.fromisoformat(last_ts_str.replace("Z", "+00:00"))
                win_end  = last_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
                win_start= (last_dt - timedelta(days=INTRADAY_BPDAY_WINDOW)).strftime(
                    "%Y-%m-%dT%H:%M:%SZ")
            except Exception:
                continue

            # Group by day using to_str (confirmed working in QuestDB)
            bpd_sql = f"""
                SELECT to_str(timestamp, 'yyyy-MM-dd') AS day, count() AS bars
                FROM   {TABLE_STOCK}
                WHERE  symbol   = '{sym}'
                AND    interval = '{iv}'
                AND    timestamp >= '{win_start}'
                AND    timestamp <= '{win_end}'
                GROUP  BY day
                ORDER  BY day
            """
            bpd_rows, bpd_err = http_query(bpd_sql, pause=0.5)
            if bpd_rows is None:
                continue

            if not bpd_rows:
                continue

            # Exclude partial first/last day
            bars_counts = [r[1] for r in bpd_rows[1:-1]] if len(bpd_rows) > 2 else [r[1] for r in bpd_rows]
            if not bars_counts:
                continue
            avg_bpd = sum(bars_counts) / len(bars_counts)

            # Flag if average bars/day is <70% of expected
            if avg_bpd < expected * 0.70:
                low_symbols.append((sym, round(avg_bpd, 1), expected))

        total_sampled = len(sample)
        if low_symbols:
            warning(f"  [{iv}] {len(low_symbols)}/{total_sampled} sampled symbols "
                    f"have <70% of expected {expected} bars/day:")
            for sym, avg, exp in low_symbols:
                info(f"    {sym:<20} avg={avg:.1f}  expected≥{exp*0.70:.0f}")
            stage_ok = False
        else:
            passed(f"  [{iv}] All {total_sampled} sampled symbols meet "
                   f"≥70% of expected {expected} bars/day.")

    # ── 4d: Intraday duplicate check (sampled per interval) ──────────────────
    info("")
    info(f"{BOLD}4d — Intraday duplicate timestamps  (sampled per interval){RESET}")
    info("")

    DUP_WIN_INTRA = 30  # calendar days

    for iv in INTRADAY_INTERVALS:
        meta = interval_meta.get(iv)
        if not meta or not meta["rows"]:
            continue

        pool   = sorted(meta["rows"], key=lambda r: -(r[2] or 0))
        sample = pool[:INTRADAY_SAMPLE]

        dup_found_syms = []
        for sym, last_ts_str, _ in sample:
            try:
                last_dt  = datetime.fromisoformat(last_ts_str.replace("Z", "+00:00"))
                win_end  = last_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
                win_start= (last_dt - timedelta(days=DUP_WIN_INTRA)).strftime(
                    "%Y-%m-%dT%H:%M:%SZ")
            except Exception:
                continue

            dup_sql = f"""
                SELECT timestamp, cnt
                FROM (
                    SELECT timestamp, count() AS cnt
                    FROM   {TABLE_STOCK}
                    WHERE  symbol   = '{sym}'
                    AND    interval = '{iv}'
                    AND    timestamp >= '{win_start}'
                    AND    timestamp <= '{win_end}'
                    GROUP  BY timestamp
                )
                WHERE cnt > 1
                LIMIT  5
            """
            dup_rows, dup_err = http_query(dup_sql, pause=0.4)
            if dup_rows is None:
                continue
            if dup_rows:
                dup_found_syms.append((sym, dup_rows))

        if dup_found_syms:
            failed(f"  [{iv}] Duplicates found in "
                   f"{len(dup_found_syms)}/{len(sample)} sampled symbols:")
            for sym, drows in dup_found_syms:
                info(f"    {sym}: {len(drows)} duplicate timestamps "
                     f"(e.g. {drows[0][0]}  ×{drows[0][1]})")
            stage_ok = False
        else:
            passed(f"  [{iv}] No duplicate timestamps in "
                   f"{len(sample)} sampled symbols.")

    # ── 4e: Intraday API spot-check (1h only, 1 random symbol) ──────────────
    info("")
    info(f"{BOLD}4e — Intraday API spot-check  (1h, 2 random symbols){RESET}")
    info("")

    meta_1h = interval_meta.get("1h")
    if not meta_1h or not meta_1h["rows"]:
        warning("  No 1h metadata available — skipping API spot-check.")
        return stage_ok

    # Pick 2 random symbols from the pool
    pool_1h = [r for r in meta_1h["rows"] if r[1]]
    sample_1h = random.sample(pool_1h, min(2, len(pool_1h)))

    for sym, last_ts_str, _ in sample_1h:
        info(f"  {'─'*60}")
        info(f"  Symbol: {BOLD}{sym}{RESET}  interval=1h")

        try:
            last_dt  = datetime.fromisoformat(last_ts_str.replace("Z", "+00:00"))
            to_ts_dt = last_dt
            fr_ts_dt = last_dt - timedelta(days=INTRADAY_API_WINDOW)
            to_ts    = int(to_ts_dt.timestamp())
            fr_ts    = int(fr_ts_dt.timestamp())
            fr_str   = fr_ts_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            to_str_  = to_ts_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        except Exception as e:
            warning(f"  Could not compute window for {sym}: {e}")
            continue

        info(f"  Window: {fr_ts_dt.strftime('%Y-%m-%d %H:%M')} → "
             f"{to_ts_dt.strftime('%Y-%m-%d %H:%M')} UTC  ({INTRADAY_API_WINDOW} days)")

        # Fetch from API
        time.sleep(0.3)
        api_data = fetch_intraday_api(sym, "1h", fr_ts, to_ts)

        if api_data == "FORBIDDEN":
            warning(f"  [{sym}] EODHD intraday API returned 403 Forbidden — "
                    "plan may restrict historical intraday access. Skipping.")
            continue
        if api_data is None:
            warning(f"  [{sym}] API call failed — skipping.")
            continue

        # Build API map: datetime_str -> OHLCV
        api_map: dict = {}
        for row in api_data:
            # EODHD intraday uses Unix 'timestamp' + 'datetime' fields
            raw_ts = row.get("timestamp") or row.get("datetime", "")
            if isinstance(raw_ts, int):
                key = datetime.utcfromtimestamp(raw_ts).strftime("%Y-%m-%d %H:%M")
            else:
                key = str(raw_ts)[:16]  # trim to "yyyy-MM-dd HH:MM"
            api_map[key] = {
                "open":   float(row.get("open",   0) or 0),
                "high":   float(row.get("high",   0) or 0),
                "low":    float(row.get("low",    0) or 0),
                "close":  float(row.get("close",  0) or 0),
                "volume": int(  row.get("volume", 0) or 0),
            }

        # Fetch from DB
        db_sql = (
            f"SELECT timestamp, open, high, low, close, volume "
            f"FROM {TABLE_STOCK} "
            f"WHERE symbol='{sym}' AND interval='1h' "
            f"AND timestamp >= '{fr_str}' AND timestamp <= '{to_str_}' "
            f"ORDER BY timestamp LIMIT 500"
        )
        try:
            db_rows = pg_query(db_sql)
        except Exception as e:
            warning(f"  DB query failed: {e}")
            continue

        db_map: dict = {}
        for ts, o, h, l, c, v in (db_rows or []):
            if hasattr(ts, "strftime"):
                key = ts.strftime("%Y-%m-%d %H:%M")
            else:
                key = str(ts)[:16]
            db_map[key] = {
                "open":   float(o or 0), "high":  float(h or 0),
                "low":    float(l or 0), "close": float(c or 0),
                "volume": int(v or 0),
            }

        info(f"  API bars: {len(api_map):>5}  |  DB bars: {len(db_map):>5}")

        all_keys    = sorted(set(api_map) | set(db_map))
        only_in_api = [k for k in all_keys if k in api_map and k not in db_map]
        only_in_db  = [k for k in all_keys if k in db_map and k not in api_map]

        PRICE_TOL = 0.01
        discrepancies = []
        for k in all_keys:
            if k not in api_map or k not in db_map:
                continue
            a, b = api_map[k], db_map[k]
            diffs = []
            for field in ("open", "high", "low", "close"):
                if abs(a[field] - b[field]) > PRICE_TOL:
                    diffs.append(f"{field}: API={a[field]:.4f} DB={b[field]:.4f}")
            if a["volume"] != b["volume"]:
                diffs.append(f"volume: API={a['volume']:,} DB={b['volume']:,}")
            if diffs:
                discrepancies.append((k, diffs))

        sym_ok = True
        if only_in_api:
            sym_ok = False; stage_ok = False
            warning(f"  {len(only_in_api)} bar(s) in API but missing from DB:")
            for k in only_in_api[:3]:
                info(f"    {k}  close={api_map[k]['close']}")
            if len(only_in_api) > 3:
                info(f"    … and {len(only_in_api)-3} more")

        if only_in_db:
            info(f"  {len(only_in_db)} bar(s) in DB but not in API (timezone mismatch / adjusted) — not flagged")

        if discrepancies:
            sym_ok = False; stage_ok = False
            warning(f"  {len(discrepancies)} bar(s) with OHLCV discrepancies:")
            for k, diffs in discrepancies[:3]:
                info(f"    {k}: {'; '.join(diffs)}")

        if sym_ok:
            passed(f"  {sym} 1h: DB matches API — consistent.")
        else:
            failed(f"  {sym} 1h: discrepancies found (see above).")

    return stage_ok


# ════════════════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════════════════
def main():
    print(f"\n{'═'*70}")
    print(f"{BOLD}  EODHD Data Quality Validator  —  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}{RESET}")
    print(f"  QuestDB PG : {QUESTDB_HOST}:{QUESTDB_PG_PORT}")
    print(f"  QuestDB HTTP: {QUESTDB_HOST}:{QUESTDB_HTTP_PORT}")
    print(f"  Table       : {TABLE_STOCK}  ({TABLE_META})")
    print(f"{'═'*70}")

    if not EODHD_API_KEY:
        print(f"\n{RED}[FATAL] EODHD_API_KEY not set. Add it to .env or export it.{RESET}")
        sys.exit(1)

    # Verify HTTP connectivity before running stages
    rows, err = http_query("SELECT 1", pause=0.0)
    if rows is None:
        print(f"\n{RED}[FATAL] Cannot reach QuestDB HTTP API at "
              f"{HTTP_BASE}: {err}{RESET}")
        sys.exit(1)

    results = {}

    def run_stage(label, fn):
        try:
            results[label] = fn()
        except Exception as e:
            failed(f"Unexpected error in {label}: {e}")
            results[label] = False

    run_stage("Stage 1 — EOD Duplicates",        stage1_duplicates)
    run_stage("Stage 2 — EOD Missing Data",      stage2_missing)
    run_stage("Stage 3 — EOD API Consistency",   stage3_consistency)
    run_stage("Stage 4 — Intraday Quality",      stage4_intraday)

    # ── Summary ──────────────────────────────────────────────────────────────
    print(f"\n{'═'*70}")
    print(f"{BOLD}  SUMMARY REPORT{RESET}")
    print(f"{'═'*70}")
    all_passed = True
    for stage, ok in results.items():
        status = f"{GREEN}PASSED{RESET}" if ok else f"{RED}FAILED{RESET}"
        print(f"  {stage:<40} →  {status}")
        if not ok:
            all_passed = False
    print(f"{'═'*70}")
    if all_passed:
        print(f"\n{GREEN}{BOLD}  Overall: ALL CHECKS PASSED — data quality is good.{RESET}\n")
    else:
        print(f"\n{YELLOW}{BOLD}  Overall: ISSUES DETECTED — review warnings/failures above.{RESET}\n")


if __name__ == "__main__":
    main()
