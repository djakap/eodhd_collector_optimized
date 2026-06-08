"""
Price Data Collector
Collects price data for all intervals (5m, 15m, 30m, 1h, 1d, 1w, 1M)
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime, timedelta
from typing import List, Dict, Optional
import logging
import numpy as np
import pandas as pd

from api.eodhd_client import EODHDClient
from db.questdb_client import QuestDBClient
from utils.data_filter import filter_and_validate
from config.eodhd_config import INTRADAY_INTERVALS, EOD_PERIODS, INTRADAY_MAX_DAYS

logger = logging.getLogger(__name__)


class PriceCollector:
    """Collects price data from EODHD API"""
    
    def __init__(self, skip_validation: bool = False, update_mode: bool = False, 
                 update_window: int = 7, symbols_to_preload: List[str] = None,
                 skip_duplicate_check: bool = False):
        self.api_client = EODHDClient()
        self.db_client = QuestDBClient()
        self.db_client.connect()
        self.skip_validation = skip_validation  # Skip OHLC validation for speed
        self.update_mode = update_mode  # Incremental update mode
        self.update_window = update_window  # Days to re-fetch for corrections
        self.skip_duplicate_check = skip_duplicate_check  # Skip duplicate detection (for fresh collections)
        
        # Pre-load timestamps for all symbols (ONLY in update mode to avoid duplicates)
        # For fresh collection, on-demand queries are faster
        self.preloaded_timestamps = {}
        if symbols_to_preload and self.update_mode:
            self._preload_timestamps(symbols_to_preload)
    
    def _preload_timestamps(self, symbols: List[str]):
        """Pre-load existing timestamps for all symbols to avoid repeated queries"""
        logger.info(f"Pre-loading timestamps for {len(symbols)} symbols...")
        loaded = 0
        for symbol in symbols:
            # Pre-load EOD periods
            for period in EOD_PERIODS:
                key = f"{symbol}_{period}"
                timestamps = self.db_client.get_existing_timestamps(symbol, period)
                if timestamps:  # Only store if not empty
                    self.preloaded_timestamps[key] = timestamps
                    loaded += 1
            # Pre-load intraday intervals
            for interval in INTRADAY_INTERVALS:
                key = f"{symbol}_{interval}"
                timestamps = self.db_client.get_existing_timestamps(symbol, interval)
                if timestamps:  # Only store if not empty
                    self.preloaded_timestamps[key] = timestamps
                    loaded += 1
        logger.info(f"Pre-loading complete: {loaded} interval(s) with existing data")
    
    def close(self):
        """Close database connection"""
        self.db_client.close()
    
    def collect_eod_data(self, symbol: str, days: int = None) -> int:
        """
        Collect EOD data (1d, 1w, 1M) with update mode support
        
        Args:
            symbol: Stock symbol (e.g., 'BBCA.JK')
            days: Number of days to fetch (None = all available, ignored in update mode)
        
        Returns:
            Number of records inserted
        """
        total_records = 0
        current_time = datetime.now()
        
        for period in EOD_PERIODS:
            logger.info(f"Collecting EOD data for {symbol} (period={period})")
            
            # Determine date range based on mode
            from_date = None
            use_duplicate_detection = not self.skip_duplicate_check  # Override if skip flag set
            
            if self.update_mode:
                # Get latest date from metadata
                last_date = self.db_client.get_max_timestamp(symbol, period)
                
                if last_date:
                    # Incremental update: fetch from (last_date - update_window) to today
                    from_date = last_date - timedelta(days=self.update_window)
                    
                    logger.info(f"Update mode: fetching from {from_date.date()} (last: {last_date.date()}, window: {self.update_window} days)")
                # else: no data yet - full collection
            elif days:
                # Manual days limit (not update mode)
                from_date = datetime.now() - timedelta(days=days)
            
            # Fetch data
            data = self.api_client.get_eod_data(symbol, period=period, from_date=from_date)
            
            if not data:
                logger.warning(f"No EOD data for {symbol} (period={period})")
                continue
            
            # Get existing timestamps if needed
            existing_timestamps = set()
            if use_duplicate_detection:
                # Use pre-loaded timestamps if available, otherwise query
                key = f"{symbol}_{period}"
                existing_timestamps = self.preloaded_timestamps.get(key) or self.db_client.get_existing_timestamps(symbol, period)
            
            # Pandas-based processing (10-50x faster than loops)
            try:
                # Convert to DataFrame for vectorized operations
                df = pd.DataFrame(data)
                
                # Vectorized datetime parsing (much faster than loop)
                df['timestamp'] = pd.to_datetime(df['date'], format='%Y-%m-%d', errors='coerce')
                
                # Drop records with invalid dates
                initial_count = len(df)
                df = df.dropna(subset=['timestamp'])
                
                # Duplicate detection (vectorized)
                if use_duplicate_detection and existing_timestamps:
                    df = df[~df['timestamp'].isin(existing_timestamps)]
                    skipped_duplicates = initial_count - len(df) - (initial_count - df['timestamp'].notna().sum())
                else:
                    skipped_duplicates = 0
                
                # NULL filtering (vectorized)
                before_null_filter = len(df)
                df = df.dropna(subset=['open', 'high', 'low', 'close'])
                skipped_nulls = before_null_filter - len(df)
                
                # Convert to records (tuples for database insert)
                records = []
                timestamps = []
                
                for _, row in df.iterrows():
                    records.append((
                        symbol,
                        period,
                        row['timestamp'].to_pydatetime(),
                        float(row['open']) if pd.notna(row['open']) else None,
                        float(row['high']) if pd.notna(row['high']) else None,
                        float(row['low']) if pd.notna(row['low']) else None,
                        float(row['close']) if pd.notna(row['close']) else None,
                        float(row['adjusted_close']) if pd.notna(row.get('adjusted_close')) else None,
                        int(row['volume']) if pd.notna(row.get('volume')) else None,
                        None,  # gmtoffset
                        'eod',
                        current_time
                    ))
                    timestamps.append(row['timestamp'].to_pydatetime())
                
                logger.debug(f"Used pandas optimization for {initial_count} EOD records")
                
            except Exception as e:
                # Fallback to loop-based processing if pandas fails
                logger.warning(f"Pandas processing failed, using fallback: {e}")
                records = []
                timestamps = []
                skipped_duplicates = 0
                skipped_nulls = 0
                
                for item in data:
                    date_str = item.get('date')
                    if not date_str:
                        continue
                    
                    try:
                        timestamp = datetime.strptime(date_str, '%Y-%m-%d')
                    except ValueError:
                        continue
                    
                    # Duplicate check
                    if use_duplicate_detection and timestamp in existing_timestamps:
                        skipped_duplicates += 1
                        continue
                    
                    # NULL check
                    if not all([item.get('open'), item.get('high'), item.get('low'), item.get('close')]):
                        skipped_nulls += 1
                        continue
                    
                    # Append directly (single iteration)
                    records.append((
                        symbol,
                        period,
                        timestamp,
                        item.get('open'),
                        item.get('high'),
                        item.get('low'),
                        item.get('close'),
                        item.get('adjusted_close'),
                        item.get('volume'),
                        None,  # gmtoffset
                        'eod',
                        current_time
                    ))
                    timestamps.append(timestamp)
            
            # Log skipped records
            if skipped_duplicates > 0:
                logger.info(f"Skipped {skipped_duplicates} existing records for {symbol} ({period})")
            if skipped_nulls > 0:
                logger.info(f"Skipped {skipped_nulls} NULL records for {symbol} ({period})")
            
            if records:
                self.db_client.insert_price_data(records)
                total_records += len(records)
                logger.info(f"Inserted {len(records)} EOD records for {symbol} (period={period})")
                
                # Update metadata for tracking
                self.db_client.upsert_stock_metadata(
                    symbol, period,
                    data_start=min(timestamps),
                    data_end=max(timestamps),
                    total_records=len(records)
                )
        
        return total_records
    
    def collect_intraday_data(self, symbol: str, days: int = INTRADAY_MAX_DAYS) -> int:
        """
        Collect intraday data (5m, 15m, 30m, 1h) with update mode support
        
        Args:
            symbol: Stock symbol (e.g., 'BBCA.JK')
            days: Number of days to fetch (default: 600, ignored in update mode)
        
        Returns:
            Number of records inserted
        """
        total_records = 0
        current_time = datetime.now()
        
        for interval in INTRADAY_INTERVALS:
            logger.info(f"Collecting intraday data for {symbol} (interval={interval})")
            
            # Determine timestamp range based on mode
            use_duplicate_detection = True
            
            if self.update_mode:
                # Get latest timestamp from metadata
                last_datetime = self.db_client.get_max_timestamp(symbol, interval)
                
                if last_datetime:
                    # Incremental update: fetch from (last_datetime - update_window days) to now
                    from_datetime = last_datetime - timedelta(days=self.update_window)
                    from_ts = int(from_datetime.timestamp())
                    to_ts = int(datetime.now().timestamp())
                    
                    logger.info(f"Update mode: fetching from {from_datetime} (last: {last_datetime}, window: {self.update_window} days)")
                else:
                    # No data yet - full collection
                    to_ts = int(datetime.now().timestamp())
                    from_ts = int((datetime.now() - timedelta(days=days)).timestamp())
            else:
                # Manual days limit (not update mode)
                to_ts = int(datetime.now().timestamp())
                from_ts = int((datetime.now() - timedelta(days=days)).timestamp())
            
            # Fetch data
            data = self.api_client.get_intraday_data(
                symbol, 
                interval=interval,
                from_timestamp=from_ts,
                to_timestamp=to_ts
            )
            
            if not data:
                logger.warning(f"No intraday data for {symbol} (interval={interval})")
                continue
            
            # Get existing timestamps if needed
            existing_timestamps = set()
            if use_duplicate_detection:
                # Use pre-loaded timestamps if available, otherwise query
                key = f"{symbol}_{interval}"
                existing_timestamps = self.preloaded_timestamps.get(key) or self.db_client.get_existing_timestamps(symbol, interval)
            
            # Optimized processing: Try pandas first (most efficient), fallback to numpy, then loops
            try:
                # Pandas processing (10-50x faster, works for all dataset sizes)
                df = pd.DataFrame(data)
                
                # Convert timestamp to datetime
                df['datetime'] = pd.to_datetime(df['timestamp'], unit='s', errors='coerce')
                
                # Drop invalid timestamps
                initial_count = len(df)
                df = df.dropna(subset=['datetime'])
                
                # Duplicate detection (vectorized)
                if use_duplicate_detection and existing_timestamps:
                    df = df[~df['datetime'].isin(existing_timestamps)]
                    skipped_duplicates = initial_count - len(df) - (initial_count - df['datetime'].notna().sum())
                else:
                    skipped_duplicates = 0
                
                # Smart NULL filtering with market hours check
                # Extract hour for market hours check
                df['hour'] = df['datetime'].dt.hour
                
                # Identify records with NULL values
                df['has_null'] = df[['open', 'high', 'low', 'close']].isna().any(axis=1)
                
                # Keep records that are: (not null) OR (null during market hours 9-16)
                df['is_market_hours'] = (df['hour'] >= 9) & (df['hour'] < 16)
                df['keep'] = ~df['has_null'] | (df['has_null'] & df['is_market_hours'])
                
                # Count nulls filtered out
                skipped_nulls = len(df[df['has_null'] & ~df['is_market_hours']])
                
                # Filter
                df = df[df['keep']]
                
                # Convert to records (tuples for database insert)
                records = []
                timestamps = []
                
                for _, row in df.iterrows():
                    records.append((
                        symbol,
                        interval,
                        row['datetime'].to_pydatetime(),
                        float(row['open']) if pd.notna(row['open']) else None,
                        float(row['high']) if pd.notna(row['high']) else None,
                        float(row['low']) if pd.notna(row['low']) else None,
                        float(row['close']) if pd.notna(row['close']) else None,
                        None,  # adjusted_close
                        int(row['volume']) if pd.notna(row.get('volume')) else None,
                        int(row['gmtoffset']) if pd.notna(row.get('gmtoffset')) else None,
                        'intraday',
                        current_time
                    ))
                    timestamps.append(row['datetime'].to_pydatetime())
                
                logger.debug(f"Used pandas optimization for {initial_count} intraday records")
                
            except Exception as e:
                # Fallback to numpy for large datasets, then loops
                logger.debug(f"Pandas processing failed, trying numpy: {e}")
                
                try:
                    if len(data) > 1000:
                        # Use numpy for vectorized operations (3-5x faster)
                        ts_array = np.array([item.get('timestamp') for item in data if item.get('timestamp')])
                        opens = np.array([item.get('open') for item in data if item.get('timestamp')])
                        highs = np.array([item.get('high') for item in data if item.get('timestamp')])
                        lows = np.array([item.get('low') for item in data if item.get('timestamp')])
                        closes = np.array([item.get('close') for item in data if item.get('timestamp')])
                        volumes = np.array([item.get('volume') for item in data if item.get('timestamp')])
                        gmtoffsets = np.array([item.get('gmtoffset') for item in data if item.get('timestamp')])
                        
                        # Vectorized NULL check
                        has_nulls = np.isnan(opens.astype(float)) | np.isnan(highs.astype(float)) | \
                                   np.isnan(lows.astype(float)) | np.isnan(closes.astype(float))
                        
                        # Vectorized hour extraction
                        hours = np.array([datetime.fromtimestamp(ts).hour for ts in ts_array])
                        is_market_hours = (hours >= 9) & (hours < 16)
                        
                        # Keep records that are: (not null) OR (null during market hours)
                        keep_mask = ~has_nulls | (has_nulls & is_market_hours)
                        
                        # Count skipped
                        skipped_nulls = np.sum(has_nulls & ~is_market_hours)
                        
                        # Build records from filtered data
                        records = []
                        timestamps = []
                        skipped_duplicates = 0
                        
                        for idx in np.where(keep_mask)[0]:
                            timestamp = datetime.fromtimestamp(ts_array[idx])
                            
                            # Duplicate check
                            if use_duplicate_detection and timestamp in existing_timestamps:
                                skipped_duplicates += 1
                                continue
                            
                            records.append((
                                symbol,
                                interval,
                                timestamp,
                                float(opens[idx]) if not np.isnan(opens[idx]) else None,
                                float(highs[idx]) if not np.isnan(highs[idx]) else None,
                                float(lows[idx]) if not np.isnan(lows[idx]) else None,
                                float(closes[idx]) if not np.isnan(closes[idx]) else None,
                                None,  # adjusted_close
                                int(volumes[idx]) if not np.isnan(volumes[idx]) else None,
                                int(gmtoffsets[idx]) if not np.isnan(gmtoffsets[idx]) else None,
                                'intraday',
                                current_time
                            ))
                            timestamps.append(timestamp)
                        
                        logger.debug(f"Used numpy optimization for {len(data)} records")
                    else:
                        # Small dataset, use regular processing
                        raise ValueError("Small dataset, use loop processing")
                        
                except Exception as e2:
                    # Final fallback: Loop-based filtering
                    logger.debug(f"Numpy processing failed, using loops: {e2}")
                    records = []
                    timestamps = []
                    skipped_duplicates = 0
                    skipped_nulls = 0
                    
                    for item in data:
                        ts = item.get('timestamp')
                        if not ts:
                            continue
                        
                        timestamp = datetime.fromtimestamp(ts)
                        
                        # Duplicate check
                        if use_duplicate_detection and timestamp in existing_timestamps:
                            skipped_duplicates += 1
                            continue
                        
                        # Smart NULL filtering
                        has_null = not all([item.get('open'), item.get('high'), 
                                           item.get('low'), item.get('close')])
                        
                        if has_null:
                            hour = timestamp.hour
                            is_market_hours = 9 <= hour < 16
                            if not is_market_hours:
                                skipped_nulls += 1
                                continue
                        
                        # Append directly (single iteration)
                        records.append((
                            symbol,
                            interval,
                            timestamp,
                            item.get('open'),
                            item.get('high'),
                            item.get('low'),
                            item.get('close'),
                            None,  # adjusted_close
                            item.get('volume'),
                            item.get('gmtoffset'),
                            'intraday',
                            current_time
                        ))
                        timestamps.append(timestamp)
            
            # Log skipped records
            if skipped_duplicates > 0:
                logger.info(f"Skipped {skipped_duplicates} existing records for {symbol} ({interval})")
            if skipped_nulls > 0:
                logger.info(f"Skipped {skipped_nulls} NULL records for {symbol} ({interval})")
            
            if records:
                self.db_client.insert_price_data(records)
                total_records += len(records)
                logger.info(f"Inserted {len(records)} intraday records for {symbol} (interval={interval})")
                
                # Update metadata for tracking
                self.db_client.upsert_stock_metadata(
                    symbol, interval,
                    data_start=min(timestamps),
                    data_end=max(timestamps),
                    total_records=len(records)
                )
        
        return total_records
    
    def collect_all_intervals(self, symbol: str, intraday_days: int = INTRADAY_MAX_DAYS) -> Dict:
        """
        Collect all price data (EOD + Intraday)
        
        Args:
            symbol: Stock symbol (e.g., 'BBCA.JK')
            intraday_days: Number of days for intraday data
        
        Returns:
            Dictionary with collection stats
        """
        logger.info(f"Starting price collection for {symbol}")
        
        stats = {
            'symbol': symbol,
            'eod_records': 0,
            'intraday_records': 0,
            'total_records': 0,
            'success': False,
            'error': None
        }
        
        try:
            # Collect EOD data (all history)
            eod_count = self.collect_eod_data(symbol)
            stats['eod_records'] = eod_count
            
            # Collect intraday data (600 days)
            intraday_count = self.collect_intraday_data(symbol, days=intraday_days)
            stats['intraday_records'] = intraday_count
            
            stats['total_records'] = eod_count + intraday_count
            stats['success'] = True
            
            # Update metadata
            self.db_client.insert_or_update_metadata(symbol, {
                'last_price_update': datetime.now(),
                'total_price_records': stats['total_records']
            })
            
            logger.info(f"✅ Completed {symbol}: {stats['total_records']} total records")
            
        except Exception as e:
            stats['error'] = str(e)
            logger.error(f"❌ Failed to collect {symbol}: {e}")
        
        return stats


if __name__ == "__main__":
    # Test with single stock
    from utils.logger import setup_logging
    setup_logging()
    
    collector = PriceCollector()
    try:
        stats = collector.collect_all_intervals('BBCA.JK', intraday_days=7)
        print(f"\nCollection Stats: {stats}")
    finally:
        collector.close()
