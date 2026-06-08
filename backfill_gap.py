#!/usr/bin/env python3
"""
Backfill two missing data windows for all symbols in QuestDB:

  1. December 2024  – the partition was dropped to remove duplicates created
                      when backfill_nov2024.py re-inserted data that the
                      regular collector had already committed.

  2. 2026-02-28 → today – the Prefect worker has been stopped; this gap
                           fills the ~99 days since last live collection.

Usage:
    python3 backfill_gap.py                # full run (both windows)
    python3 backfill_gap.py --dry-run      # fetch only, no writes
    python3 backfill_gap.py --symbol BBCA.JK   # single symbol
    python3 backfill_gap.py --eod-only     # skip intraday (faster)
    python3 backfill_gap.py --window1-only # only December 2024
    python3 backfill_gap.py --window2-only # only the 2026 gap
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import argparse
from datetime import datetime, timezone, date
import logging
import time
import pandas as pd

from api.eodhd_client import EODHDClient
from db.questdb_client import QuestDBClient
from utils.logger import setup_logging

# ── Window 1: December 2024 (partition was dropped to remove duplicates) ────
W1_EOD_FROM  = datetime(2024, 11, 28)   # buffer: catches weekly bars starting late Nov
W1_EOD_TO    = datetime(2024, 12, 31)
W1_INTRA_FROM = int(datetime(2024, 12,  1,  0,  0, 0, tzinfo=timezone.utc).timestamp())
W1_INTRA_TO   = int(datetime(2024, 12, 31, 23, 59, 59, tzinfo=timezone.utc).timestamp())

# ── Window 2: 2026-02-28 → today (Prefect worker gap) ───────────────────────
W2_EOD_FROM  = datetime(2026, 2, 27)    # slight overlap with last good data
W2_EOD_TO    = datetime.today()
W2_INTRA_FROM = int(datetime(2026, 2, 28, 0, 0, 0, tzinfo=timezone.utc).timestamp())
W2_INTRA_TO   = int(datetime.now(tz=timezone.utc).timestamp())

EOD_PERIODS        = ['d', 'w', 'm']
INTRADAY_INTERVALS = ['5m', '15m', '30m', '1h']

CHUNK = 300   # rows per SQL INSERT — keeps partition I/O bounded

logger = logging.getLogger(__name__)


# ── Helpers ───────────────────────────────────────────────────────────────────

def get_all_symbols(db_client: QuestDBClient) -> list[str]:
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
            symbol, period,
            row['timestamp'].to_pydatetime(),
            float(row['open']), float(row['high']),
            float(row['low']),  float(row['close']),
            float(row['adjusted_close']) if pd.notna(row.get('adjusted_close')) else None,
            int(row['volume'])           if pd.notna(row.get('volume'))          else None,
            None, 'eod', now,
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
            symbol, interval,
            row['dt'].to_pydatetime(),
            float(row['open'])    if pd.notna(row.get('open'))    else None,
            float(row['high'])    if pd.notna(row.get('high'))    else None,
            float(row['low'])     if pd.notna(row.get('low'))     else None,
            float(row['close'])   if pd.notna(row.get('close'))   else None,
            None,
            int(row['volume'])    if pd.notna(row.get('volume'))  else None,
            int(row['gmtoffset']) if pd.notna(row.get('gmtoffset')) else None,
            'intraday', now,
        ))
    return records


def chunked_insert(db: QuestDBClient, records: list) -> None:
    """Insert records in small batches with brief pauses to avoid OOM."""
    for start in range(0, len(records), CHUNK):
        chunk = records[start:start + CHUNK]
        db.insert_price_data(chunk)
        if start + CHUNK < len(records):
            time.sleep(0.3)


def wait_for_db(max_wait: int = 90) -> bool:
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


# ── Per-symbol backfill for one window ───────────────────────────────────────

def backfill_symbol_window(symbol: str, api: EODHDClient, db: QuestDBClient,
                           dry_run: bool,
                           eod_from: datetime, eod_to: datetime,
                           intra_from: int, intra_to: int,
                           do_eod: bool, do_intraday: bool) -> int:
    inserted = 0

    if do_eod:
        for period in EOD_PERIODS:
            try:
                data = api.get_eod_data(symbol, period,
                                        from_date=eod_from, to_date=eod_to)
                records = to_eod_records(data or [], symbol, period)
                if records and not dry_run:
                    chunked_insert(db, records)
                inserted += len(records)
            except Exception as e:
                logger.warning(f"  {symbol} EOD {period}: {e}")

    if do_intraday:
        for interval in INTRADAY_INTERVALS:
            try:
                data = api.get_intraday_data(symbol, interval,
                                             from_timestamp=intra_from,
                                             to_timestamp=intra_to)
                records = to_intraday_records(data or [], symbol, interval)
                if records and not dry_run:
                    chunked_insert(db, records)
                inserted += len(records)
            except Exception as e:
                logger.warning(f"  {symbol} intraday {interval}: {e}")

    return inserted


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Backfill Dec 2024 and 2026-02-28 to today")
    parser.add_argument('--dry-run',      action='store_true')
    parser.add_argument('--symbol',       metavar='SYM')
    parser.add_argument('--eod-only',     action='store_true')
    parser.add_argument('--intraday-only', action='store_true')
    parser.add_argument('--window1-only', action='store_true', help="Only December 2024")
    parser.add_argument('--window2-only', action='store_true', help="Only 2026-02-28 to today")
    parser.add_argument('--verbose',      action='store_true')
    args = parser.parse_args()

    setup_logging()
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    do_eod      = not args.intraday_only
    do_intraday = not args.eod_only

    windows = []
    if not args.window2_only:
        windows.append({
            'label': 'Window 1: December 2024',
            'eod_from':   W1_EOD_FROM,  'eod_to':   W1_EOD_TO,
            'intra_from': W1_INTRA_FROM, 'intra_to': W1_INTRA_TO,
        })
    if not args.window1_only:
        windows.append({
            'label': f'Window 2: 2026-02-28 → {W2_EOD_TO.strftime("%Y-%m-%d")}',
            'eod_from':   W2_EOD_FROM,  'eod_to':   W2_EOD_TO,
            'intra_from': W2_INTRA_FROM, 'intra_to': W2_INTRA_TO,
        })

    print("=" * 70)
    print("BACKFILL: December 2024 + March–June 2026 gap")
    for w in windows:
        print(f"  {w['label']}")
    if args.dry_run:    print("MODE: DRY RUN (no writes)")
    if args.eod_only:   print("INTERVALS: EOD only")
    if args.intraday_only: print("INTERVALS: Intraday only")
    print("=" * 70)

    db  = QuestDBClient(use_ilp=False)
    db.connect()
    api = EODHDClient()

    try:
        symbols = [args.symbol] if args.symbol else get_all_symbols(db)
        total = len(symbols)
        print(f"Symbols to process: {total}")
        print(f"Windows           : {len(windows)}")
        print()

        grand_total = 0
        failed = []

        for i, symbol in enumerate(symbols, 1):
            sym_total = 0
            try:
                for win in windows:
                    n = backfill_symbol_window(
                        symbol, api, db, dry_run=args.dry_run,
                        eod_from=win['eod_from'],   eod_to=win['eod_to'],
                        intra_from=win['intra_from'], intra_to=win['intra_to'],
                        do_eod=do_eod, do_intraday=do_intraday,
                    )
                    sym_total += n

                grand_total += sym_total
                label = f"{sym_total:>7} rows" if sym_total else "  no new data"
                print(f"[{i:3d}/{total}] {symbol:<12} {label}")
                time.sleep(0.3)

            except Exception as e:
                err_str = str(e)
                if "Connection refused" in err_str or "server closed" in err_str:
                    print(f"[{i:3d}/{total}] {symbol:<12}  QuestDB down — waiting...")
                    if wait_for_db():
                        db.connect()
                        try:
                            sym_total = 0
                            for win in windows:
                                n = backfill_symbol_window(
                                    symbol, api, db, dry_run=args.dry_run,
                                    eod_from=win['eod_from'],   eod_to=win['eod_to'],
                                    intra_from=win['intra_from'], intra_to=win['intra_to'],
                                    do_eod=do_eod, do_intraday=do_intraday,
                                )
                                sym_total += n
                            grand_total += sym_total
                            print(f"[{i:3d}/{total}] {symbol:<12} {sym_total:>7} rows (retry ok)")
                        except Exception as e2:
                            failed.append(symbol)
                            print(f"[{i:3d}/{total}] {symbol:<12}  RETRY FAILED: {e2}")
                    else:
                        failed.append(symbol)
                        print(f"[{i:3d}/{total}] {symbol:<12}  DB did not recover — skipped")
                else:
                    failed.append(symbol)
                    print(f"[{i:3d}/{total}] {symbol:<12}  ERROR: {e}")

        print()
        print(f"{'DRY RUN — ' if args.dry_run else ''}Done.")
        print(f"Total rows {'found' if args.dry_run else 'inserted'}: {grand_total:,}")
        if failed:
            print(f"Failed ({len(failed)}): {', '.join(failed[:20])}")
            if len(failed) > 20:
                print(f"  ... and {len(failed)-20} more")

    finally:
        db.close()
        api.close()


if __name__ == "__main__":
    main()
