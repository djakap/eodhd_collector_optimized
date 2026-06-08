#!/usr/bin/env python3
"""
Backfill missing November 2024 data for all symbols in QuestDB.

Confirmed gap: eodhd_stock_data has data up to 2024-10 then jumps to 2024-12.
This script fetches only the Nov 2024 window for every symbol and interval,
then inserts the results. Safe to run from inside Docker or natively.

Usage:
    python3 backfill_nov2024.py            # run backfill
    python3 backfill_nov2024.py --dry-run  # show what would be fetched, no writes
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import argparse
from datetime import datetime, timezone
import logging
import time
import pandas as pd

from api.eodhd_client import EODHDClient
from db.questdb_client import QuestDBClient
from utils.logger import setup_logging

# ── Target window ────────────────────────────────────────────────────────────
# Slight buffer on either side: weekly/monthly bars that cover Nov may have
# dates that land just outside the strict Nov 1–30 range.
EOD_FROM = datetime(2024, 10, 28)   # catches weekly bars that start in late Oct
EOD_TO   = datetime(2024, 12, 1)    # catches weekly bars that end in early Dec

# Intraday uses Unix timestamps (UTC).
INTRADAY_FROM_TS = int(datetime(2024, 11, 1,  0,  0,  0, tzinfo=timezone.utc).timestamp())
INTRADAY_TO_TS   = int(datetime(2024, 11, 30, 23, 59, 59, tzinfo=timezone.utc).timestamp())

# Intervals pulled from the API (4h is locally aggregated — handled separately)
EOD_PERIODS        = ['d', 'w', 'm']
INTRADAY_INTERVALS = ['5m', '15m', '30m', '1h']

logger = logging.getLogger(__name__)


# ── Helpers ───────────────────────────────────────────────────────────────────

def get_all_symbols(db_client: QuestDBClient) -> list[str]:
    # Use metadata table — avoids a full scan of the 19 M-row stock_data table
    # which crashes QuestDB via GROUP BY / DISTINCT on that size.
    db_client.ensure_connection()
    db_client.cursor.execute(
        "SELECT DISTINCT symbol FROM eodhd_stock_metadata WHERE interval = 'd' ORDER BY symbol"
    )
    return [row[0] for row in db_client.cursor.fetchall()]


def to_eod_records(data: list, symbol: str, period: str) -> list[tuple]:
    if not data:
        return []
    now = datetime.now()
    df = pd.DataFrame(data)
    df['timestamp'] = pd.to_datetime(df['date'], format='%Y-%m-%d', errors='coerce')
    df = df.dropna(subset=['timestamp', 'open', 'high', 'low', 'close'])

    records = []
    for _, row in df.iterrows():
        records.append((
            symbol,
            period,
            row['timestamp'].to_pydatetime(),
            float(row['open']),
            float(row['high']),
            float(row['low']),
            float(row['close']),
            float(row['adjusted_close']) if pd.notna(row.get('adjusted_close')) else None,
            int(row['volume'])           if pd.notna(row.get('volume'))          else None,
            None,       # gmtoffset
            'eod',
            now,
        ))
    return records


def to_intraday_records(data: list, symbol: str, interval: str) -> list[tuple]:
    if not data:
        return []
    now = datetime.now()
    df = pd.DataFrame(data)
    df['dt'] = pd.to_datetime(df['timestamp'], unit='s', errors='coerce')
    df = df.dropna(subset=['dt'])

    records = []
    for _, row in df.iterrows():
        records.append((
            symbol,
            interval,
            row['dt'].to_pydatetime(),
            float(row['open'])      if pd.notna(row.get('open'))      else None,
            float(row['high'])      if pd.notna(row.get('high'))      else None,
            float(row['low'])       if pd.notna(row.get('low'))       else None,
            float(row['close'])     if pd.notna(row.get('close'))     else None,
            None,                   # adjusted_close
            int(row['volume'])      if pd.notna(row.get('volume'))    else None,
            int(row['gmtoffset'])   if pd.notna(row.get('gmtoffset')) else None,
            'intraday',
            now,
        ))
    return records


# ── Per-symbol backfill ───────────────────────────────────────────────────────

def backfill_symbol(symbol: str, api: EODHDClient, db: QuestDBClient,
                    dry_run: bool, eod_only: bool = False,
                    intraday_only: bool = False) -> int:
    inserted = 0

    if not intraday_only:
        for period in EOD_PERIODS:
            try:
                data = api.get_eod_data(symbol, period, from_date=EOD_FROM, to_date=EOD_TO)
                records = to_eod_records(data or [], symbol, period)
                if records and not dry_run:
                    db.insert_price_data(records)
                inserted += len(records)
                if records:
                    logger.debug(f"  {symbol} {period}: {len(records)} rows")
            except Exception as e:
                logger.warning(f"  {symbol} {period} error: {e}")

    if eod_only:
        return inserted

    CHUNK = 300   # rows per SQL INSERT batch — keeps QuestDB partition I/O bounded

    for interval in INTRADAY_INTERVALS:
        try:
            data = api.get_intraday_data(symbol, interval,
                                         from_timestamp=INTRADAY_FROM_TS,
                                         to_timestamp=INTRADAY_TO_TS)
            records = to_intraday_records(data or [], symbol, interval)
            if records and not dry_run:
                # Insert in small chunks with a brief pause to avoid
                # overwhelming QuestDB with partition writes across 20 daily partitions
                for start in range(0, len(records), CHUNK):
                    chunk = records[start:start + CHUNK]
                    db.insert_price_data(chunk)
                    if start + CHUNK < len(records):
                        time.sleep(0.5)
            inserted += len(records)
            if records:
                logger.debug(f"  {symbol} {interval}: {len(records)} rows")
        except Exception as e:
            logger.warning(f"  {symbol} {interval} error: {e}")

    return inserted


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Backfill November 2024 data")
    parser.add_argument('--dry-run', action='store_true',
                        help="Fetch but do not write to QuestDB")
    parser.add_argument('--symbol', metavar='SYM',
                        help="Backfill a single symbol only (e.g. BBCA.JK)")
    parser.add_argument('--eod-only', action='store_true',
                        help="Skip intraday intervals (use when API returns 403 for intraday)")
    parser.add_argument('--intraday-only', action='store_true',
                        help="Skip EOD intervals (use when EOD was already backfilled)")
    parser.add_argument('--verbose', action='store_true',
                        help="Show per-interval debug output")
    args = parser.parse_args()

    setup_logging()
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    print("=" * 60)
    print("BACKFILL: November 2024 missing data")
    print(f"EOD window  : {EOD_FROM.date()} → {EOD_TO.date()}")
    print(f"Intraday UTC: 2024-11-01 → 2024-11-30")
    if args.dry_run:
        print("MODE        : DRY RUN (no writes)")
    if args.eod_only:
        print("INTERVALS   : EOD only (d, w, m) — intraday skipped")
    if args.intraday_only:
        print("INTERVALS   : Intraday only (5m,15m,30m,1h) — EOD skipped")
    print("=" * 60)

    # use_ilp=False: ILP (fast path) crashes QuestDB under memory pressure
    # with an already large table; SQL insert is slower but stable.
    db  = QuestDBClient(use_ilp=False)
    db.connect()
    api = EODHDClient()

    try:
        if args.symbol:
            symbols = [args.symbol]
        else:
            symbols = get_all_symbols(db)

        total = len(symbols)
        print(f"Symbols to process: {total}\n")

        total_inserted = 0
        failed = []

        def wait_for_db(max_wait: int = 60) -> bool:
            """Wait up to max_wait seconds for QuestDB to come back."""
            import psycopg2
            for _ in range(max_wait // 3):
                try:
                    c = psycopg2.connect(host="localhost", port=8812,
                                         user="admin", password="quest", database="qdb")
                    c.close()
                    return True
                except Exception:
                    time.sleep(3)
            return False

        for i, symbol in enumerate(symbols, 1):
            try:
                n = backfill_symbol(symbol, api, db, dry_run=args.dry_run,
                                    eod_only=args.eod_only,
                                    intraday_only=args.intraday_only)
                total_inserted += n
                label = f"{n:>6} rows" if n else "  no new data"
                print(f"[{i:3d}/{total}] {symbol:<12} {label}")
                time.sleep(0.3)  # brief pause between symbols — reduces DB pressure
            except Exception as e:
                err_str = str(e)
                if "Connection refused" in err_str or "server closed" in err_str:
                    print(f"[{i:3d}/{total}] {symbol:<12}  QuestDB down — waiting...")
                    if wait_for_db():
                        db.connect()
                        print(f"  QuestDB recovered — retrying {symbol}")
                        try:
                            n = backfill_symbol(symbol, api, db, dry_run=args.dry_run,
                                                eod_only=args.eod_only,
                                                intraday_only=args.intraday_only)
                            total_inserted += n
                            print(f"[{i:3d}/{total}] {symbol:<12} {n:>6} rows (retry ok)")
                        except Exception as e2:
                            failed.append(symbol)
                            print(f"[{i:3d}/{total}] {symbol:<12}  RETRY FAILED: {e2}")
                    else:
                        failed.append(symbol)
                        print(f"[{i:3d}/{total}] {symbol:<12}  DB did not recover — skipped")
                else:
                    failed.append(symbol)
                    print(f"[{i:3d}/{total}] {symbol:<12}  ERROR: {e}")

        print(f"\n{'DRY RUN — ' if args.dry_run else ''}Done.")
        print(f"Total rows {'found' if args.dry_run else 'inserted'}: {total_inserted}")
        if failed:
            print(f"Failed ({len(failed)}): {', '.join(failed)}")

    finally:
        db.close()
        api.close()


if __name__ == "__main__":
    main()
