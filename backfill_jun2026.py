#!/usr/bin/env python3
"""
Targeted backfill: fill the June 18-20, 2026 gap (3 missing trading days).
Safe to re-run — QuestDB DEDUP prevents duplicates.

Usage:
    python3 -u backfill_jun2026.py
    python3 -u backfill_jun2026.py --eod-only
    python3 -u backfill_jun2026.py --symbol BBCA.JK
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import argparse, time
from datetime import datetime, timezone

from api.eodhd_client import EODHDClient
from db.questdb_client import QuestDBClient
from utils.logger import setup_logging
from backfill_gap import (
    get_all_symbols, to_eod_records, to_intraday_records, chunked_insert, wait_for_db
)

EOD_PERIODS        = ['d', 'w', 'm']
INTRADAY_INTERVALS = ['5m', '15m', '30m', '1h']

EOD_FROM   = datetime(2026, 6, 17)        # slight overlap with last good data
EOD_TO     = datetime(2026, 6, 21)        # today (Saturday)
INTRA_FROM = int(datetime(2026, 6, 17, 0, 0, 0, tzinfo=timezone.utc).timestamp())
INTRA_TO   = int(datetime.now(tz=timezone.utc).timestamp())


def backfill_symbol(symbol, api, db, do_eod, do_intraday):
    inserted = 0

    if do_eod:
        for period in EOD_PERIODS:
            try:
                data = api.get_eod_data(symbol, period, from_date=EOD_FROM, to_date=EOD_TO)
                records = to_eod_records(data or [], symbol, period)
                if records:
                    chunked_insert(db, records)
                inserted += len(records)
            except Exception as e:
                print(f'  WARN {symbol} EOD {period}: {e}')

    if do_intraday:
        for iv in INTRADAY_INTERVALS:
            try:
                data = api.get_intraday_data(symbol, iv,
                                             from_timestamp=INTRA_FROM,
                                             to_timestamp=INTRA_TO)
                records = to_intraday_records(data or [], symbol, iv)
                if records:
                    chunked_insert(db, records)
                inserted += len(records)
            except Exception as e:
                print(f'  WARN {symbol} intraday {iv}: {e}')

    return inserted


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--symbol', metavar='SYM')
    parser.add_argument('--eod-only',      action='store_true')
    parser.add_argument('--intraday-only', action='store_true')
    parser.add_argument('--dry-run',       action='store_true')
    args = parser.parse_args()

    setup_logging()
    do_eod      = not args.intraday_only
    do_intraday = not args.eod_only

    print('=' * 65)
    print('BACKFILL: June 18-20, 2026 (3 missing trading days)')
    print(f'  EOD range    : {EOD_FROM.date()} → {EOD_TO.date()}')
    print(f'  Intraday from: {datetime.utcfromtimestamp(INTRA_FROM).date()}')
    print(f'  EOD      : {do_eod}  |  Intraday: {do_intraday}')
    if args.dry_run:
        print('  MODE: DRY RUN (no writes)')
    print('=' * 65)

    db  = QuestDBClient(use_ilp=False)
    db.connect()
    api = EODHDClient()

    try:
        symbols = [args.symbol] if args.symbol else get_all_symbols(db)
        total   = len(symbols)
        print(f'Symbols: {total}\n')

        grand_total = 0
        failed      = []

        for i, symbol in enumerate(symbols, 1):
            try:
                n = 0 if args.dry_run else backfill_symbol(symbol, api, db, do_eod, do_intraday)
                grand_total += n
                label = f'{n:>7} rows' if n else '  no new data'
                print(f'[{i:3d}/{total}] {symbol:<12} {label}', flush=True)
                time.sleep(0.2)

            except Exception as e:
                err = str(e)
                if 'Connection refused' in err or 'server closed' in err:
                    print(f'[{i:3d}/{total}] {symbol:<12}  DB down — waiting...')
                    if wait_for_db():
                        db.connect()
                        try:
                            n = backfill_symbol(symbol, api, db, do_eod, do_intraday)
                            grand_total += n
                            print(f'[{i:3d}/{total}] {symbol:<12} {n:>7} rows (retry ok)')
                        except Exception as e2:
                            failed.append(symbol)
                            print(f'[{i:3d}/{total}] {symbol:<12}  RETRY FAILED: {e2}')
                    else:
                        failed.append(symbol)
                        print(f'[{i:3d}/{total}] {symbol:<12}  DB did not recover')
                else:
                    failed.append(symbol)
                    print(f'[{i:3d}/{total}] {symbol:<12}  ERROR: {e}')

        print()
        print(f'Done. Total rows inserted: {grand_total:,}')
        if failed:
            print(f'Failed ({len(failed)}): {", ".join(failed[:20])}')

    finally:
        db.close()
        api.close()


if __name__ == '__main__':
    main()
