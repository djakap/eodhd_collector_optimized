"""
Backfill November 2024 data for all symbols.

The 2024-11 partition was lost due to WAL corruption (now fixed).
Fetches EOD + intraday from EODHD API and aggregates 4h candles.

Usage:
    python scripts/backfill_nov2024.py
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import time, logging
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

from api.eodhd_client import EODHDClient
from db.questdb_client import QuestDBClient
from config.eodhd_config import INTRADAY_INTERVALS, EOD_PERIODS
from utils.aggregate_4h import aggregate_4h_candles
from utils.logger import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

FROM_DATE = datetime(2024, 11, 1)
TO_DATE   = datetime(2024, 11, 30)
FROM_TS   = int(datetime(2024, 11, 1,  0,  0,  0, tzinfo=timezone.utc).timestamp())
TO_TS     = int(datetime(2024, 11, 30, 23, 59, 59, tzinfo=timezone.utc).timestamp())

WORKERS    = 5
print_lock = Lock()


def get_symbols() -> list:
    db = QuestDBClient()
    db.connect()
    try:
        db.cursor.execute("SELECT DISTINCT symbol FROM eodhd_stock_metadata ORDER BY symbol")
        return [r[0] for r in db.cursor.fetchall()]
    finally:
        db.close()


def nov_exists(db: QuestDBClient, symbol: str, interval: str) -> set:
    """Return timestamps already in the Nov 2024 window for this symbol/interval."""
    try:
        db.cursor.execute("""
            SELECT timestamp FROM eodhd_stock_data
            WHERE symbol = %s AND interval = %s
              AND timestamp >= '2024-11-01' AND timestamp < '2024-12-01'
        """, (symbol, interval))
        return {r[0] for r in db.cursor.fetchall()}
    except Exception:
        return set()


def backfill_symbol(symbol: str) -> dict:
    api = EODHDClient()
    db  = QuestDBClient()
    db.connect()
    now  = datetime.now()
    stats = {'symbol': symbol, 'eod': 0, 'intraday': 0, 'errors': []}

    try:
        # ── EOD: d / w / m ────────────────────────────────────────────────
        for period in EOD_PERIODS:
            try:
                data = api.get_eod_data(symbol, period=period,
                                        from_date=FROM_DATE, to_date=TO_DATE)
                if not data:
                    continue

                existing = nov_exists(db, symbol, period)
                records  = []

                for item in data:
                    date_str = item.get('date', '')
                    try:
                        ts = datetime.strptime(date_str, '%Y-%m-%d')
                    except ValueError:
                        continue
                    if ts in existing:
                        continue
                    if not all([item.get('open'), item.get('high'),
                                item.get('low'),  item.get('close')]):
                        continue
                    records.append((
                        symbol, period, ts,
                        float(item['open']),
                        float(item['high']),
                        float(item['low']),
                        float(item['close']),
                        float(item['adjusted_close']) if item.get('adjusted_close') else None,
                        int(item['volume'])            if item.get('volume')         else None,
                        None, 'eod', now,
                    ))

                if records:
                    db.insert_price_data(records)
                    stats['eod'] += len(records)

            except Exception as e:
                stats['errors'].append(f"eod/{period}: {e}")

        # ── Intraday: 5m / 15m / 30m / 1h ────────────────────────────────
        for interval in INTRADAY_INTERVALS:
            try:
                data = api.get_intraday_data(symbol, interval=interval,
                                             from_timestamp=FROM_TS,
                                             to_timestamp=TO_TS)
                if not data:
                    continue

                existing = nov_exists(db, symbol, interval)
                records  = []

                for item in data:
                    raw_ts = item.get('timestamp')
                    if not raw_ts:
                        continue
                    ts = datetime.fromtimestamp(raw_ts)
                    if ts in existing:
                        continue
                    has_null = not all([item.get('open'), item.get('high'),
                                        item.get('low'),  item.get('close')])
                    if has_null and not (9 <= ts.hour < 16):
                        continue
                    records.append((
                        symbol, interval, ts,
                        float(item['open'])  if item.get('open')      else None,
                        float(item['high'])  if item.get('high')      else None,
                        float(item['low'])   if item.get('low')       else None,
                        float(item['close']) if item.get('close')     else None,
                        None,
                        int(item['volume'])     if item.get('volume')    else None,
                        int(item['gmtoffset'])  if item.get('gmtoffset') else None,
                        'intraday', now,
                    ))

                if records:
                    db.insert_price_data(records)
                    stats['intraday'] += len(records)

            except Exception as e:
                err = str(e)
                if '403' not in err:          # 403 = expected (outside history window)
                    stats['errors'].append(f"intraday/{interval}: {err}")

    finally:
        db.close()
        api.close()

    return stats


def main():
    symbols = get_symbols()
    total   = len(symbols)
    logger.info(f"Backfilling Nov 2024 for {total} symbols (EOD + intraday)...")

    done = eod_total = intraday_total = err_syms = 0
    start = time.time()

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {pool.submit(backfill_symbol, sym): sym for sym in symbols}
        for fut in as_completed(futures):
            s = fut.result()
            done += 1
            eod_total      += s['eod']
            intraday_total += s['intraday']
            if s['errors']:
                err_syms += 1

            if done % 25 == 0 or done == total:
                elapsed = time.time() - start
                eta = (total - done) / (done / elapsed) if done else 0
                with print_lock:
                    print(f"[{done:>4}/{total}]  eod={eod_total:,}  "
                          f"intraday={intraday_total:,}  errors={err_syms}  "
                          f"eta={eta/60:.1f}m", flush=True)

    elapsed = time.time() - start
    print(f"\n── Fetch complete in {elapsed/60:.1f} min ──")
    print(f"  EOD records:      {eod_total:,}")
    print(f"  Intraday records: {intraday_total:,}")
    print(f"  Error symbols:    {err_syms}")

    # ── 4h aggregation from any 1h data inserted ──────────────────────────
    if intraday_total > 0:
        print("\nAggregating 4h candles from Nov 2024 1h data...")
        result = aggregate_4h_candles(since='2024-11-01', symbols=symbols)
        print(f"  4h candles inserted: {result['total_candles']:,} "
              f"for {result['symbols_processed']} symbols")
    else:
        print("\nNo intraday data inserted — skipping 4h aggregation.")
        print("(EODHD intraday history window doesn't reach Nov 2024)")


if __name__ == '__main__':
    main()
