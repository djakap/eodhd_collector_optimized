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
    db_client.ensure_connection()
    db_client.cursor.execute(
        "SELECT DISTINCT symbol FROM eodhd_stock_data ORDER BY symbol"
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

    for interval in INTRADAY_INTERVALS:
        try:
            data = api.get_intraday_data(symbol, interval,
                                         from_timestamp=INTRADAY_FROM_TS,
                                         to_timestamp=INTRADAY_TO_TS)
            records = to_intraday_records(data or [], symbol, interval)
            if records and not dry_run:
                db.insert_price_data(records)
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

    db  = QuestDBClient()
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

        for i, symbol in enumerate(symbols, 1):
            try:
                n = backfill_symbol(symbol, api, db, dry_run=args.dry_run,
                                    eod_only=args.eod_only,
                                    intraday_only=args.intraday_only)
                total_inserted += n
                label = f"{n:>6} rows" if n else "  no new data"
                print(f"[{i:3d}/{total}] {symbol:<12} {label}")
            except Exception as e:
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
