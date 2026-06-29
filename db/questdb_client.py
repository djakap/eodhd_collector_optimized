"""
QuestDB Client
Handles database operations with ILP (InfluxDB Line Protocol) support for 10-100x faster inserts
"""

import psycopg2
from psycopg2 import pool
from psycopg2.extras import execute_batch
from typing import List, Dict, Optional, Set, Tuple
from datetime import datetime, timedelta
import logging
import time
import threading

try:
    from questdb.ingress import Sender, Protocol, IngressError, TimestampNanos
    HAS_ILP = True
except ImportError:
    HAS_ILP = False
    TimestampNanos = None
    logger = logging.getLogger(__name__)  # Initialize logger here if import fails

from config.db_config import (
    PG_CONNECTION_STRING,
    TABLE_STOCK_DATA,
    TABLE_CORPORATE_ACTIONS,
    TABLE_CALENDAR_EVENTS,
    TABLE_METADATA,
    TABLE_STOCK_METADATA,
    BATCH_INSERT_SIZE,
    QUESTDB_HOST,
    QUESTDB_INFLUX_PORT
)

logger = logging.getLogger(__name__)


class QuestDBClient:
    """Client for QuestDB operations with ILP support for maximum performance"""
    
    # Class-level connection pool (shared across all instances)
    _connection_pool = None
    _pool_lock = threading.Lock()
    _use_ilp = HAS_ILP  # Class variable to control ILP usage
    _ilp_lock = threading.Lock()  # Lock for ILP operations (thread-safe)
    
    def __init__(self, use_ilp: bool = True):
        self.connection_string = PG_CONNECTION_STRING
        self.conn = None
        self.cursor = None
        self.use_ilp = use_ilp and QuestDBClient._use_ilp  # Instance setting
        self.ilp_host = QUESTDB_HOST
        self.ilp_port = QUESTDB_INFLUX_PORT
        
        # Initialize connection pool (singleton pattern)
        if QuestDBClient._connection_pool is None:
            with QuestDBClient._pool_lock:
                if QuestDBClient._connection_pool is None:
                    try:
                        QuestDBClient._connection_pool = pool.ThreadedConnectionPool(
                            minconn=1,
                            maxconn=20,  # Support up to 20 parallel workers
                            **self._parse_connection_string(PG_CONNECTION_STRING)
                        )
                        logger.info("Created QuestDB connection pool (1-20 connections)")
                        
                        # Test ILP connection if available
                        if HAS_ILP and use_ilp:
                            try:
                                with Sender(Protocol.Tcp, self.ilp_host, self.ilp_port) as sender:
                                    pass  # Just test connection
                                logger.info(f"✅ QuestDB ILP available at {self.ilp_host}:{self.ilp_port} (10-100x faster inserts)")
                            except Exception as e:
                                logger.warning(f"ILP connection test failed, will use SQL: {e}")
                                QuestDBClient._use_ilp = False
                                self.use_ilp = False
                        
                    except Exception as e:
                        logger.warning(f"Failed to create connection pool: {e}, using single connection")
                        QuestDBClient._connection_pool = None
    
    def _parse_connection_string(self, conn_str: str) -> dict:
        """Parse PostgreSQL connection string to dict"""
        # postgresql://user:password@host:port/database
        import re
        match = re.match(r'postgresql://([^:]+):([^@]+)@([^:]+):(\d+)/(.+)', conn_str)
        if match:
            return {
                'user': match.group(1),
                'password': match.group(2),
                'host': match.group(3),
                'port': int(match.group(4)),
                'database': match.group(5)
            }
        return {}
    
    def connect(self, retries=3, backoff=1.0):
        """Establish database connection (from pool if available) with retry logic"""
        last_error = None
        for attempt in range(retries):
            try:
                if QuestDBClient._connection_pool is not None:
                    # Get connection from pool
                    self.conn = QuestDBClient._connection_pool.getconn()
                    self.conn.autocommit = True
                else:
                    # Fallback to single connection
                    self.conn = psycopg2.connect(self.connection_string)
                    self.conn.autocommit = True
                
                self.cursor = self.conn.cursor()
                logger.debug("Connected to QuestDB")
                return  # Success
                
            except Exception as e:
                last_error = e
                # If connection refused, the pool may have stale connections
                # or QuestDB may have restarted — reset the pool so next attempt
                # creates fresh connections
                if 'Connection refused' in str(e) or 'server closed the connection' in str(e):
                    self._reset_pool()
                if attempt < retries - 1:
                    wait_time = backoff * (2 ** attempt)  # Exponential backoff
                    logger.warning(f"Connection attempt {attempt + 1}/{retries} failed, retrying in {wait_time:.1f}s: {e}")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Failed to connect to QuestDB after {retries} attempts: {e}")
                    raise last_error

    @classmethod
    def close_pool(cls):
        """Close all pooled connections and drop the pool.

        Call this once at the very end of a run (e.g. after a flow finishes).
        The pool is class-level/shared and otherwise keeps up to maxconn
        connections open for the lifetime of the process — which leaks
        memory in long-lived workers that run many flows back-to-back."""
        with cls._pool_lock:
            if cls._connection_pool is not None:
                try:
                    cls._connection_pool.closeall()
                    logger.info("Closed QuestDB connection pool")
                except Exception as e:
                    logger.warning(f"Error closing connection pool: {e}")
                finally:
                    cls._connection_pool = None

    @classmethod
    def _reset_pool(cls):
        """Reset the connection pool when QuestDB becomes unreachable.
        Next connect() call will recreate the pool with fresh connections."""
        with cls._pool_lock:
            if cls._connection_pool is not None:
                try:
                    cls._connection_pool.closeall()
                except Exception:
                    pass
                cls._connection_pool = None
                logger.warning("Connection pool reset — will recreate on next connect")
    
    def ensure_connection(self, retries=3):
        """Ensure database connection is alive, reconnect if needed with retry logic"""
        try:
            # Check if connection exists and is alive
            if self.conn is None or self.conn.closed or self.cursor is None or self.cursor.closed:
                logger.warning("Database connection lost, reconnecting...")
                self.connect(retries=retries)
        except Exception as e:
            logger.error(f"Failed to ensure connection: {e}")
            self.connect(retries=retries)
    
    def close(self):
        """Close database connection (return to pool if using pool)"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            if QuestDBClient._connection_pool is not None:
                # Return connection to pool
                QuestDBClient._connection_pool.putconn(self.conn)
                logger.debug("Returned connection to pool")
            else:
                # Close single connection
                self.conn.close()
                logger.info("Disconnected from QuestDB")
    
    def get_existing_timestamps(self, symbol: str, interval: str, retries=2) -> set:
        """
        Get existing timestamps for a symbol/interval to avoid duplicates
        
        Args:
            symbol: Stock symbol (e.g., 'BBCA.JK')
            interval: Data interval (e.g., 'd', '5m')
            retries: Number of retry attempts for mmap failures
        
        Returns:
            Set of existing timestamps
        """
        last_error = None
        for attempt in range(retries):
            try:
                self.ensure_connection()
                self.cursor.execute(f"""
                    SELECT DISTINCT timestamp 
                    FROM {TABLE_STOCK_DATA} 
                    WHERE symbol = %s AND interval = %s
                """, (symbol, interval))
                return {row[0] for row in self.cursor.fetchall()}
            except Exception as e:
                last_error = e
                if attempt < retries - 1:
                    # Retry on mmap failures (memory pressure)
                    if "mmap" in str(e).lower():
                        time.sleep(0.5)  # Brief pause for memory to free up
                        logger.debug(f"Retrying timestamp query for {symbol}/{interval} (attempt {attempt + 2}/{retries})")
                        continue
                # Log warning only on final failure
                logger.warning(f"Could not query existing timestamps for {symbol}/{interval}: {last_error}")
                return set()  # Return empty set, duplicates handled by QuestDB
    
    def get_max_timestamp(self, symbol: str, interval: str) -> Optional[datetime]:
        """
        Get the latest data timestamp for a stock/interval from metadata
        
        Args:
            symbol: Stock symbol
            interval: Data interval
        
        Returns:
            Latest timestamp or None if no metadata exists
        """
        try:
            self.ensure_connection()
            # Get latest metadata record (in case of duplicates)
            self.cursor.execute(f"""
                SELECT data_end 
                FROM {TABLE_STOCK_METADATA} 
                WHERE symbol = %s AND interval = %s
                ORDER BY last_updated DESC
                LIMIT 1
            """, (symbol, interval))
            
            result = self.cursor.fetchone()
            return result[0] if result else None
        except Exception as e:
            logger.warning(f"Could not query max timestamp for {symbol}/{interval}: {e}")
            return None
    
    def delete_records_after_date(self, symbol: str, interval: str, from_date: datetime):
        """
        No-op kept for backward compatibility.

        QuestDB does not support row-level DELETE (the old ``DELETE FROM`` here
        always failed with "unexpected token [FROM]"). The eodhd_stock_data table
        has deduplication enabled with UPSERT KEYS (symbol, interval, timestamp),
        so re-inserting bars in the correction window overwrites the existing rows
        in place. An explicit delete is therefore unnecessary.

        Args:
            symbol: Stock symbol
            interval: Data interval
            from_date: Start of the correction window (informational only)
        """
        logger.debug(
            f"Skipping delete for {symbol}/{interval} from {from_date}: "
            f"QuestDB DEDUP (symbol, interval, timestamp) overwrites rows on re-insert"
        )
    
    def upsert_stock_metadata(self, symbol: str, interval: str, 
                             data_start: datetime, data_end: datetime, 
                             total_records: int):
        """
        Insert or update stock metadata for tracking data freshness
        
        Args:
            symbol: Stock symbol
            interval: Data interval
            data_start: Earliest timestamp in data
            data_end: Latest timestamp in data
            total_records: Total number of records
        """
        try:
            self.ensure_connection()
            now = datetime.now()
            today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)

            # Check if we already have a row for today (designated timestamp = last_updated)
            self.cursor.execute(f"""
                SELECT last_updated FROM {TABLE_STOCK_METADATA}
                WHERE symbol = %s AND interval = %s AND last_updated >= %s
                ORDER BY last_updated DESC LIMIT 1
            """, (symbol, interval, today_start))
            existing = self.cursor.fetchone()

            if existing:
                # UPDATE the existing row in today's partition to avoid accumulation
                self.cursor.execute(f"""
                    UPDATE {TABLE_STOCK_METADATA}
                    SET total_records = %s, data_start = %s, data_end = %s
                    WHERE symbol = %s AND interval = %s AND last_updated = %s
                """, (total_records, data_start, data_end, symbol, interval, existing[0]))
            else:
                self.cursor.execute(f"""
                    INSERT INTO {TABLE_STOCK_METADATA}
                    (symbol, interval, last_updated, total_records, data_start, data_end, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (symbol, interval, now, total_records, data_start, data_end, now))

            logger.debug(f"Updated metadata for {symbol}/{interval}: {total_records} records, latest: {data_end}")
        except Exception as e:
            # Don't raise - metadata tracking is optional
            logger.warning(f"Could not upsert metadata for {symbol}/{interval}: {e}")
    
    def get_stocks_to_update(self, symbols: List[str], intervals: List[str], 
                            max_age_days: int = 1) -> Dict[str, List[str]]:
        """
        Get stocks that need updating based on data age
        
        Args:
            symbols: List of stock symbols
            intervals: List of intervals to check
            max_age_days: Maximum age in days before considering data stale
        
        Returns:
            Dict mapping symbols to list of intervals that need updating
        """
        cutoff_date = datetime.now() - timedelta(days=max_age_days)
        stocks_to_update = {}
        
        try:
            for symbol in symbols:
                intervals_to_update = []
                
                for interval in intervals:
                    # Check if metadata exists and is fresh
                    self.cursor.execute(f"""
                        SELECT data_end
                        FROM {TABLE_STOCK_METADATA}
                        WHERE symbol = %s AND interval = %s
                        ORDER BY last_updated DESC
                        LIMIT 1
                    """, (symbol, interval))
                    
                    result = self.cursor.fetchone()
                    
                    if not result or not result[0]:
                        # No metadata - needs full collection
                        intervals_to_update.append(interval)
                    elif result[0] < cutoff_date:
                        # Data is stale - needs update
                        intervals_to_update.append(interval)
                    # else: data is fresh - skip
                
                if intervals_to_update:
                    stocks_to_update[symbol] = intervals_to_update
            
            return stocks_to_update
            
        except Exception as e:
            logger.error(f"Failed to get stocks to update: {e}")
            # On error, return all stocks (safe fallback)
            return {symbol: intervals for symbol in symbols}
    
    def check_data_freshness(self, symbols: List[str], interval: str = 'd',
                              max_age_minutes: int = 60) -> Dict[str, list]:
        """
        Bulk freshness check for screener integration.
        
        Uses a single efficient query to check which tickers have fresh data
        and which need updating.
        
        Args:
            symbols: List of stock symbols to check
            interval: Data interval to check ('d', '5m', '1h', etc.)
            max_age_minutes: Maximum age in minutes before data is considered stale
                             For daily data: 1440 (24h) is reasonable
                             For intraday: 60-120 minutes
        
        Returns:
            Dict with keys:
                'fresh': list of symbols with up-to-date data
                'stale': list of symbols with outdated data  
                'unknown': list of symbols with no metadata (never collected)
        """
        result = {'fresh': [], 'stale': [], 'unknown': []}
        cutoff = datetime.now() - timedelta(minutes=max_age_minutes)
        
        try:
            self.ensure_connection()
            
            # Build a lookup of latest data_end per symbol from metadata
            # Use a single query for all symbols (much faster than N queries)
            if not symbols:
                return result
            
            placeholders = ','.join(['%s'] * len(symbols))
            self.cursor.execute(f"""
                SELECT symbol, data_end, last_updated
                FROM {TABLE_STOCK_METADATA}
                WHERE symbol IN ({placeholders})
                AND interval = %s
                ORDER BY symbol, last_updated DESC
            """, (*symbols, interval))
            
            rows = self.cursor.fetchall()
            
            # Build lookup: symbol -> latest data_end (first row per symbol due to ORDER BY DESC)
            seen = set()
            metadata_map = {}
            for row in rows:
                sym = row[0]
                if sym not in seen:
                    metadata_map[sym] = {
                        'data_end': row[1],
                        'last_updated': row[2]
                    }
                    seen.add(sym)
            
            # Classify each symbol
            for symbol in symbols:
                if symbol not in metadata_map:
                    result['unknown'].append(symbol)
                elif metadata_map[symbol]['data_end'] is None:
                    result['unknown'].append(symbol)
                elif metadata_map[symbol]['data_end'] < cutoff:
                    result['stale'].append(symbol)
                else:
                    result['fresh'].append(symbol)
            
            logger.info(
                f"Freshness check ({interval}, {max_age_minutes}min): "
                f"{len(result['fresh'])} fresh, {len(result['stale'])} stale, "
                f"{len(result['unknown'])} unknown"
            )
            return result
            
        except Exception as e:
            logger.error(f"Freshness check failed: {e}")
            # Safe fallback: treat all as stale
            return {'fresh': [], 'stale': list(symbols), 'unknown': []}
    
    def insert_price_data(self, records):
        """
        Insert price data records using ILP (fastest) or SQL fallback
        
        Args:
            records: List of tuples (symbol, interval, timestamp, open, high, low, 
                    close, adjusted_close, volume, gmtoffset, source, created_at)
                    OR List of dicts (backward compatible)
        """
        if not records:
            return
        
        # Try ILP first (10-100x faster) if enabled
        if self.use_ilp and QuestDBClient._use_ilp:
            try:
                self._insert_price_data_ilp(records)
                return  # Success!
            except Exception as e:
                error_msg = str(e)
                # Only disable ILP permanently if it's a configuration issue, not a transient error
                if 'Connection refused' in error_msg or 'Name or service not known' in error_msg:
                    logger.warning(f"ILP configuration error, disabling permanently: {e}")
                    QuestDBClient._use_ilp = False
                    self.use_ilp = False
                else:
                    # Transient error - just log and fall back to SQL for this insert
                    logger.debug(f"ILP insert failed (transient), using SQL fallback: {e}")
        
        # Fallback to SQL insert (still fast with execute_batch)
        self._insert_price_data_sql(records)
    
    def _insert_price_data_ilp(self, records):
        """Insert using QuestDB ILP protocol (10-100x faster than SQL)"""
        if not HAS_ILP:
            raise ImportError("questdb library not available")
        
        # Convert records to appropriate format
        if records and isinstance(records[0], dict):
            # Dict format
            rows = records
        else:
            # Tuple format - convert to dicts for easier processing
            rows = []
            for r in records:
                rows.append({
                    'symbol': r[0],
                    'interval': r[1],
                    'timestamp': r[2],
                    'open': r[3],
                    'high': r[4],
                    'low': r[5],
                    'close': r[6],
                    'adjusted_close': r[7],
                    'volume': r[8],
                    'gmtoffset': r[9],
                    'source': r[10],
                    'created_at': r[11]
                })
        
        # Use thread lock to prevent concurrent ILP operations (prevents "Broken pipe")
        # Multiple threads sharing one ILP connection causes connection errors
        with QuestDBClient._ilp_lock:
            # Retry logic for transient errors
            max_retries = 2
            for attempt in range(max_retries):
                try:
                    # Create new connection with auto_flush for better reliability
                    with Sender(Protocol.Tcp, self.ilp_host, self.ilp_port, auto_flush_rows=500) as sender:
                        for row in rows:
                            # Convert to TimestampNanos (QuestDB ILP requirement)
                            if isinstance(row['timestamp'], datetime):
                                ts_nanos = TimestampNanos(int(row['timestamp'].timestamp() * 1_000_000_000))
                            else:
                                ts_nanos = TimestampNanos(int(row['timestamp'] * 1_000_000_000))
                            
                            # Build ILP row - all values must be correct types
                            # volume must be int (LONG in QuestDB), gmtoffset must be int (INT in QuestDB)
                            # Sending float for integer columns causes ILP cast error and row rejection
                            sender.row(
                                TABLE_STOCK_DATA,
                                symbols={
                                    'symbol': str(row['symbol']),
                                    'interval': str(row['interval']),
                                    'source': str(row.get('source', 'unknown'))
                                },
                                columns={
                                    'open': float(row['open']) if row.get('open') is not None else None,
                                    'high': float(row['high']) if row.get('high') is not None else None,
                                    'low': float(row['low']) if row.get('low') is not None else None,
                                    'close': float(row['close']) if row.get('close') is not None else None,
                                    'adjusted_close': float(row['adjusted_close']) if row.get('adjusted_close') is not None else None,
                                    'volume': int(row['volume']) if row.get('volume') is not None else None,
                                    'gmtoffset': int(row['gmtoffset']) if row.get('gmtoffset') is not None else None,
                                },
                                at=ts_nanos
                            )
                        
                        # Explicit flush at the end (auto_flush handles batches)
                        sender.flush()
                    
                    logger.info(f"Inserted {len(records)} price records via ILP (fast)")
                    return  # Success - exit function
                    
                except Exception as e:
                    error_msg = str(e)
                    if attempt < max_retries - 1 and ('Broken pipe' in error_msg or 'Connection' in error_msg):
                        # Retry on connection errors
                        logger.debug(f"ILP retry (attempt {attempt + 1}/{max_retries}): {error_msg}")
                        time.sleep(0.1)  # Brief delay before retry
                        continue
                    else:
                        # Final attempt failed or non-connection error
                        raise Exception(f"ILP insert failed after {attempt + 1} attempts: {error_msg}")
    
    def _insert_price_data_sql(self, records):
        """Insert using SQL (slower but compatible fallback)"""
        sql = f"""
        INSERT INTO {TABLE_STOCK_DATA} 
        (symbol, interval, timestamp, open, high, low, close, adjusted_close, 
         volume, gmtoffset, source, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        # Check if records are tuples or dicts (backward compatibility)
        if records and isinstance(records[0], dict):
            # Convert dicts to tuples for backward compatibility
            values = []
            for record in records:
                values.append((
                    record['symbol'],
                    record['interval'],
                    record['timestamp'],
                    record['open'],
                    record['high'],
                    record['low'],
                    record['close'],
                    record.get('adjusted_close'),
                    record['volume'],
                    record.get('gmtoffset'),
                    record['source'],
                    datetime.now()
                ))
        else:
            # Already tuples - use directly
            values = records
        
        try:
            # Ensure connection is alive before batch insert
            self.ensure_connection()
            # Use execute_batch for better performance with large datasets
            execute_batch(self.cursor, sql, values, page_size=BATCH_INSERT_SIZE)
            logger.info(f"Inserted {len(records)} price records via SQL")
            
            # Small delay for large batches to prevent overwhelming QuestDB
            if len(records) > 5000:
                time.sleep(0.2)
                
        except Exception as e:
            logger.error(f"Failed to insert price data: {e}")
            # Try to reconnect and retry once with delay
            try:
                logger.warning("Reconnecting and retrying insert after delay...")
                time.sleep(1)  # Wait 1 second before reconnecting
                self.connect()
                time.sleep(0.5)  # Wait before retry
                execute_batch(self.cursor, sql, values, page_size=BATCH_INSERT_SIZE)
                logger.info(f"Inserted {len(records)} price records (after reconnect)")
                
                # Delay after successful retry
                if len(records) > 5000:
                    time.sleep(0.3)
                    
            except Exception as e2:
                logger.error(f"Failed to insert price data after reconnect: {e2}")
                raise
    
    def insert_corporate_actions(self, records: List[Dict]):
        """Insert corporate actions (dividends/splits)"""
        if not records:
            return
        
        sql = f"""
        INSERT INTO {TABLE_CORPORATE_ACTIONS}
        (symbol, action_type, action_date, dividend_amount, dividend_currency,
         payment_date, record_date, declaration_date, dividend_type,
         split_ratio, split_from, split_to, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        values = []
        for record in records:
            values.append((
                record['symbol'],
                record['action_type'],
                record['action_date'],
                record.get('dividend_amount'),
                record.get('dividend_currency'),
                record.get('payment_date'),
                record.get('record_date'),
                record.get('declaration_date'),
                record.get('dividend_type'),
                record.get('split_ratio'),
                record.get('split_from'),
                record.get('split_to'),
                datetime.now()
            ))
        
        try:
            execute_batch(self.cursor, sql, values, page_size=BATCH_INSERT_SIZE)
            logger.info(f"Inserted {len(records)} corporate action records")
        except Exception as e:
            logger.error(f"Failed to insert corporate actions: {e}")
            raise
    
    def insert_or_update_metadata(self, symbol: str, data: Dict):
        """Insert or update stock metadata"""
        if not data:
            return
        
        # Simple insert (QuestDB doesn't support UPSERT easily)
        # We'll just insert new records
        try:
            sql = f"""
            INSERT INTO {TABLE_METADATA}
            (symbol, exchange, name, sector, industry, currency,
             last_price_update, has_dividends, is_active, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            now = datetime.now()
            values = (
                symbol,
                data.get('exchange', 'JK'),
                data.get('name', ''),
                data.get('sector', ''),
                data.get('industry', ''),
                data.get('currency', 'IDR'),
                data.get('last_price_update', now),
                data.get('has_dividends', False),
                True,  # is_active
                now,
                now
            )
            
            try:
                self.cursor.execute(sql, values)
                logger.debug(f"Inserted metadata for {symbol}")
            except Exception as e:
                logger.error(f"Failed to insert metadata for {symbol}: {e}")
                raise
        except Exception as e:
            logger.error(f"Failed to insert/update metadata: {e}")
