"""
Screener Smart Refresh
On-demand OHLCV data refresh for screener integration.

Checks data freshness in QuestDB, fetches only stale tickers from EODHD API,
and ensures the screener always reads up-to-date data.

Usage:
    from collectors.screener_refresh import ScreenerRefresh
    
    refresher = ScreenerRefresh(max_age_minutes=60)
    result = refresher.refresh(tickers, intervals=['d'])
    print(result)
    # {'refreshed': 42, 'already_fresh': 296, 'failed': 0, 'elapsed_s': 12.3}
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import time
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Optional
import threading

from db.questdb_client import QuestDBClient
from collectors.price_collector import PriceCollector
from config.eodhd_config import INTRADAY_INTERVALS, EOD_PERIODS

logger = logging.getLogger(__name__)


# Interval mapping: user-facing → collector-internal
INTERVAL_TO_COLLECTOR = {
    'd': 'eod',      # daily  → collect_eod_data
    'w': 'eod',      # weekly → collect_eod_data  
    'm': 'eod',      # monthly → collect_eod_data
    '5m': 'intraday',
    '15m': 'intraday',
    '30m': 'intraday',
    '1h': 'intraday',
}


class ScreenerRefresh:
    """
    On-demand data refresh for screener integration.
    
    Checks QuestDB metadata for data freshness, then fetches only
    stale tickers using the existing PriceCollector in update mode.
    """
    
    def __init__(self, max_age_minutes: int = 1440, max_workers: int = 5):
        """
        Args:
            max_age_minutes: Maximum data age before considered stale.
                             Default 1440 (24 hours) for daily data.
                             Use 60-120 for intraday.
            max_workers: Number of parallel EODHD API workers.
        """
        self.max_age_minutes = max_age_minutes
        self.max_workers = min(max(1, max_workers), 10)
    
    def check_freshness(self, tickers: List[str], interval: str = 'd') -> Dict:
        """
        Check which tickers have fresh data in QuestDB.
        
        Args:
            tickers: List of stock symbols
            interval: Data interval to check ('d', '5m', '1h', etc.)
        
        Returns:
            Dict with 'fresh', 'stale', 'unknown' lists
        """
        db = QuestDBClient(use_ilp=False)
        try:
            db.connect()
            return db.check_data_freshness(
                tickers, interval, self.max_age_minutes
            )
        finally:
            db.close()
    
    def refresh(self, tickers: List[str], intervals: List[str] = None,
                force: bool = False, update_window: int = 7) -> Dict:
        """
        Refresh OHLCV data for tickers that need updating.
        
        Args:
            tickers: List of stock symbols
            intervals: Intervals to refresh (default: ['d'])
            force: Force refresh all tickers regardless of freshness
            update_window: Days to re-fetch for potential corrections
        
        Returns:
            Dict with refresh statistics:
                refreshed: number of tickers updated
                already_fresh: number of tickers skipped (data is current)
                failed: number of tickers that failed to update
                elapsed_s: total time in seconds
                details: per-ticker results
        """
        if intervals is None:
            intervals = ['d']
        
        start_time = time.time()
        stats = {
            'refreshed': 0,
            'already_fresh': 0,
            'failed': 0,
            'elapsed_s': 0.0,
            'details': []
        }
        
        # Determine which tickers need refreshing
        tickers_to_refresh = []
        
        if force:
            tickers_to_refresh = list(tickers)
            logger.info(f"Force refresh: {len(tickers_to_refresh)} tickers")
        else:
            # Check freshness for primary interval (first in list)
            primary_interval = intervals[0]
            freshness = self.check_freshness(tickers, primary_interval)
            
            tickers_to_refresh = freshness['stale'] + freshness['unknown']
            stats['already_fresh'] = len(freshness['fresh'])
            
            if not tickers_to_refresh:
                stats['elapsed_s'] = time.time() - start_time
                logger.info(
                    f"All {len(tickers)} tickers are fresh "
                    f"(max age: {self.max_age_minutes} min). No refresh needed."
                )
                return stats
            
            logger.info(
                f"Refreshing {len(tickers_to_refresh)} stale tickers "
                f"({stats['already_fresh']} already fresh)"
            )
        
        # Refresh stale tickers in parallel
        counter_lock = threading.Lock()
        completed_count = {'n': 0}
        
        def refresh_single(symbol: str) -> Dict:
            """Worker: refresh one ticker using PriceCollector."""
            ticker_result = {
                'symbol': symbol,
                'success': False,
                'records': 0,
                'error': None
            }
            
            collector = PriceCollector(
                skip_validation=True,
                update_mode=True,
                update_window=update_window,
                skip_duplicate_check=True  # Speed: trust uniqueness
            )
            
            try:
                total_records = 0
                
                for interval in intervals:
                    collect_type = INTERVAL_TO_COLLECTOR.get(interval, 'eod')
                    
                    if collect_type == 'eod':
                        records = collector.collect_eod_data(symbol)
                        total_records += records
                    elif collect_type == 'intraday':
                        # Only fetch recent intraday (7 days for screener)
                        records = collector.collect_intraday_data(
                            symbol, days=update_window
                        )
                        total_records += records
                
                ticker_result['success'] = True
                ticker_result['records'] = total_records
                
                with counter_lock:
                    completed_count['n'] += 1
                    progress = completed_count['n'] / len(tickers_to_refresh) * 100
                    logger.info(
                        f"[{completed_count['n']}/{len(tickers_to_refresh)}] "
                        f"{progress:.0f}% - {symbol}: ✅ {total_records} records"
                    )
                    
            except Exception as e:
                ticker_result['error'] = str(e)
                with counter_lock:
                    completed_count['n'] += 1
                logger.error(f"{symbol}: ❌ {e}")
                
            finally:
                collector.close()
            
            return ticker_result
        
        # Execute in parallel
        try:
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = {
                    executor.submit(refresh_single, sym): sym 
                    for sym in tickers_to_refresh
                }
                
                for future in as_completed(futures):
                    try:
                        result = future.result()
                        stats['details'].append(result)
                        
                        if result['success']:
                            stats['refreshed'] += 1
                        else:
                            stats['failed'] += 1
                            
                    except Exception as e:
                        sym = futures[future]
                        stats['failed'] += 1
                        stats['details'].append({
                            'symbol': sym,
                            'success': False,
                            'records': 0,
                            'error': str(e)
                        })
                        
        except KeyboardInterrupt:
            logger.warning("Refresh interrupted by user")
        
        stats['elapsed_s'] = round(time.time() - start_time, 1)
        
        # Summary log
        logger.info(
            f"\n{'='*60}\n"
            f"📡 SCREENER REFRESH COMPLETE\n"
            f"{'='*60}\n"
            f"  ✅ Refreshed: {stats['refreshed']}\n"
            f"  ⏭️  Already fresh: {stats['already_fresh']}\n"
            f"  ❌ Failed: {stats['failed']}\n"
            f"  ⏱️  Elapsed: {stats['elapsed_s']}s\n"
            f"{'='*60}"
        )
        
        return stats
    
    def refresh_single_ticker(self, ticker: str, intervals: List[str] = None,
                               update_window: int = 7) -> Dict:
        """
        Convenience: refresh a single ticker (always forces refresh).
        
        Args:
            ticker: Stock symbol
            intervals: Intervals to refresh
            update_window: Days to re-fetch
        
        Returns:
            Dict with refresh result for this ticker
        """
        result = self.refresh(
            [ticker], intervals=intervals, force=True, 
            update_window=update_window
        )
        if result['details']:
            return result['details'][0]
        return {'symbol': ticker, 'success': False, 'error': 'No result'}
