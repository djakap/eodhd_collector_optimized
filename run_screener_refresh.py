#!/usr/bin/env python3
"""
CLI: Screener Smart Refresh

Ensures OHLCV data in QuestDB is fresh before the screener runs.
Only fetches new data for stale tickers — fast and API-efficient.

Usage:
    # Refresh daily data for Syariah stocks (only stale ones)
    python run_screener_refresh.py --stocks config/syariah_stocks.txt

    # Refresh intraday data (1h bars)
    python run_screener_refresh.py --stocks config/syariah_stocks.txt --interval 1h

    # Force full refresh (ignore freshness check)
    python run_screener_refresh.py --stocks config/syariah_stocks.txt --force

    # Refresh specific tickers
    python run_screener_refresh.py --tickers BRIS.JK,UNVR.JK,TLKM.JK

    # Dry run: only check freshness, don't fetch
    python run_screener_refresh.py --stocks config/syariah_stocks.txt --dry-run

    # Custom staleness threshold (120 minutes)
    python run_screener_refresh.py --stocks config/syariah_stocks.txt --max-age 120
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import argparse
import json
from utils.logger import setup_logging
from collectors.screener_refresh import ScreenerRefresh


def load_stocks(file_path: str) -> list:
    """Load stock symbols from file."""
    stocks = []
    with open(file_path, 'r') as f:
        for line in f:
            stock = line.strip()
            if stock and not stock.startswith('#'):
                if not stock.endswith('.JK'):
                    stock = f"{stock}.JK"
                stocks.append(stock)
    return stocks


def main():
    parser = argparse.ArgumentParser(
        description='Screener Smart Refresh — ensure OHLCV data is fresh in QuestDB',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_screener_refresh.py --stocks config/syariah_stocks.txt
  python run_screener_refresh.py --tickers BRIS.JK,UNVR.JK --interval 1h
  python run_screener_refresh.py --stocks config/syariah_stocks.txt --dry-run
        """
    )
    
    # Stock selection (mutually exclusive)
    stock_group = parser.add_mutually_exclusive_group(required=True)
    stock_group.add_argument(
        '--stocks', type=str,
        help='Path to stock list file (one symbol per line)'
    )
    stock_group.add_argument(
        '--tickers', type=str,
        help='Comma-separated list of tickers (e.g., BRIS.JK,UNVR.JK)'
    )
    
    # Refresh options
    parser.add_argument(
        '--interval', type=str, default='d',
        choices=['d', 'w', 'm', '5m', '15m', '30m', '1h'],
        help='Data interval to refresh (default: d)'
    )
    parser.add_argument(
        '--max-age', type=int, default=1440,
        help='Max data age in minutes before refresh (default: 1440 = 24h)'
    )
    parser.add_argument(
        '--update-window', type=int, default=7,
        help='Days to re-fetch for corrections (default: 7)'
    )
    parser.add_argument(
        '--workers', type=int, default=5,
        help='Parallel workers for EODHD API (default: 5, max: 10)'
    )
    parser.add_argument(
        '--force', action='store_true',
        help='Force refresh all tickers (ignore freshness check)'
    )
    parser.add_argument(
        '--dry-run', action='store_true',
        help='Only check freshness, do not fetch data'
    )
    parser.add_argument(
        '--json', action='store_true',
        help='Output results in JSON format'
    )
    
    args = parser.parse_args()
    setup_logging()
    
    # Load tickers
    if args.stocks:
        tickers = load_stocks(args.stocks)
    else:
        tickers = [t.strip() for t in args.tickers.split(',')]
    
    if not tickers:
        print("❌ No tickers to process")
        sys.exit(1)
    
    print(f"\n{'='*60}")
    print(f"📡 SCREENER SMART REFRESH")
    print(f"{'='*60}")
    print(f"  📊 Tickers: {len(tickers)}")
    print(f"  📅 Interval: {args.interval}")
    print(f"  ⏱️  Max age: {args.max_age} minutes")
    print(f"  🔄 Force: {'Yes' if args.force else 'No'}")
    print(f"  👀 Dry run: {'Yes' if args.dry_run else 'No'}")
    print(f"{'='*60}\n")
    
    refresher = ScreenerRefresh(
        max_age_minutes=args.max_age,
        max_workers=args.workers
    )
    
    if args.dry_run:
        # Only check freshness — no data fetching
        freshness = refresher.check_freshness(tickers, args.interval)
        
        if args.json:
            print(json.dumps(freshness, indent=2))
        else:
            print(f"✅ Fresh ({len(freshness['fresh'])}): {', '.join(freshness['fresh'][:10])}"
                  + (f" +{len(freshness['fresh'])-10} more" if len(freshness['fresh']) > 10 else ""))
            print(f"⚠️  Stale ({len(freshness['stale'])}): {', '.join(freshness['stale'][:10])}"
                  + (f" +{len(freshness['stale'])-10} more" if len(freshness['stale']) > 10 else ""))
            print(f"❓ Unknown ({len(freshness['unknown'])}): {', '.join(freshness['unknown'][:10])}"
                  + (f" +{len(freshness['unknown'])-10} more" if len(freshness['unknown']) > 10 else ""))
    else:
        # Full refresh
        result = refresher.refresh(
            tickers,
            intervals=[args.interval],
            force=args.force,
            update_window=args.update_window
        )
        
        if args.json:
            # Remove details for cleaner JSON output
            output = {k: v for k, v in result.items() if k != 'details'}
            print(json.dumps(output, indent=2))
        else:
            print(f"\n✅ Refreshed: {result['refreshed']}")
            print(f"⏭️  Already fresh: {result['already_fresh']}")
            print(f"❌ Failed: {result['failed']}")
            print(f"⏱️  Elapsed: {result['elapsed_s']}s")
            
            # Show failed tickers if any
            failed = [d for d in result['details'] if not d['success']]
            if failed:
                print(f"\nFailed tickers:")
                for f in failed:
                    print(f"  ❌ {f['symbol']}: {f['error']}")


if __name__ == '__main__':
    main()
