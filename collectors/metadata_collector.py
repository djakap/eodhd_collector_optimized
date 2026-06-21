"""
Stock Metadata Collector
Collects stock metadata (name, exchange, currency, ISIN) from EODHD API
"""

import logging
from datetime import datetime
from typing import List, Optional

from api.eodhd_client import EODHDClient
from db.questdb_client import QuestDBClient
from config.db_config import TABLE_METADATA

logger = logging.getLogger(__name__)


class MetadataCollector:
    """Collector for stock metadata"""
    
    def __init__(self):
        self.api_client = EODHDClient()
        self.db_client = QuestDBClient()
        self.db_client.connect()
    
    def close(self):
        """Close database connection and HTTP client"""
        try:
            self.db_client.close()
        finally:
            # Close the httpx client too — otherwise its connection pool leaks
            self.api_client.close()
    
    def collect_metadata(self, symbol: str) -> Optional[dict]:
        """
        Collect metadata for a single stock
        
        Args:
            symbol: Stock symbol (e.g., 'BBCA.JK')
        
        Returns:
            Dict with collection stats or None
        """
        try:
            logger.info(f"Collecting metadata for {symbol}")
            
            # Fetch metadata from API
            metadata = self.api_client.get_stock_metadata(symbol)
            
            if not metadata:
                logger.warning(f"No metadata found for {symbol}")
                return None
            
            # Insert into database
            self._insert_metadata(symbol, metadata)
            
            logger.info(f"✅ Collected metadata for {symbol}")
            
            return {
                'symbol': symbol,
                'name': metadata.get('Name'),
                'exchange': metadata.get('Exchange'),
                'success': True
            }
            
        except Exception as e:
            logger.error(f"Failed to collect metadata for {symbol}: {e}")
            return {
                'symbol': symbol,
                'success': False,
                'error': str(e)
            }
    
    def collect_exchange_metadata(self, exchange: str = 'JK') -> dict:
        """
        Collect metadata for all stocks in an exchange
        
        Args:
            exchange: Exchange code (default: 'JK')
        
        Returns:
            Dict with collection stats
        """
        try:
            logger.info(f"Collecting metadata for exchange {exchange}")
            
            # Fetch all symbols from exchange
            symbols_data = self.api_client.get_exchange_symbols(exchange)
            
            if not symbols_data:
                logger.error(f"No symbols found for exchange {exchange}")
                return {'success': False, 'total': 0}
            
            logger.info(f"Found {len(symbols_data)} symbols on {exchange} exchange")
            
            # Insert all metadata
            inserted = 0
            for symbol_info in symbols_data:
                try:
                    symbol = f"{symbol_info['Code']}.{exchange}"
                    self._insert_metadata(symbol, symbol_info)
                    inserted += 1
                except Exception as e:
                    logger.error(f"Failed to insert {symbol_info.get('Code')}: {e}")
            
            logger.info(f"✅ Inserted {inserted}/{len(symbols_data)} metadata records")
            
            return {
                'success': True,
                'total': len(symbols_data),
                'inserted': inserted,
                'exchange': exchange
            }
            
        except Exception as e:
            logger.error(f"Failed to collect exchange metadata: {e}")
            return {'success': False, 'error': str(e)}
    
    def _insert_metadata(self, symbol: str, metadata: dict):
        """Insert metadata into database"""
        now = datetime.now()
        
        sql = f"""
        INSERT INTO {TABLE_METADATA}
        (symbol, exchange, name, sector, industry, currency,
         last_price_update, has_dividends, is_active, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        values = (
            symbol,
            metadata.get('Exchange', 'JK'),
            metadata.get('Name', ''),
            metadata.get('Sector', ''),
            metadata.get('Industry', ''),
            metadata.get('Currency', 'IDR'),
            now,  # last_price_update
            False,  # has_dividends (will be updated by action collector)
            True,  # is_active
            now,  # created_at
            now   # updated_at
        )
        
        try:
            self.db_client.cursor.execute(sql, values)
            logger.debug(f"Inserted metadata for {symbol}")
        except Exception as e:
            logger.error(f"Failed to insert metadata for {symbol}: {e}")
            raise
