#!/usr/bin/env python3
"""
Verify the EODHD Bulk API BEFORE relying on it — run AFTER the daily quota resets
(midnight GMT). Confirms the exchange code is correct and prints the real response
field names so the parser in collectors/bulk_collector.py can be adjusted if needed.

Run inside the worker container:
    docker exec prefect-worker python3 /app/scripts/test_bulk.py

Costs ~300 API calls (3 bulk requests). Does NOT write to QuestDB — read-only.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from api.eodhd_client import EODHDClient
from config.eodhd_config import EXCHANGE_CODE


def main():
    c = EODHDClient()
    print(f"Exchange code under test: '{EXCHANGE_CODE}'\n")
    try:
        for atype in [None, 'dividends', 'splits']:
            label = atype or 'eod'
            data = c.get_bulk_eod(EXCHANGE_CODE, action_type=atype)
            if not data:
                print(f"[{label}] ❌ NO DATA / error — check exchange code '{EXCHANGE_CODE}', "
                      f"quota (402?), or that the market traded recently.")
                continue
            print(f"[{label}] ✅ {len(data)} records")
            print(f"  fields  : {list(data[0].keys())}")
            print(f"  example : {data[0]}\n")
    finally:
        c.close()


if __name__ == '__main__':
    main()
