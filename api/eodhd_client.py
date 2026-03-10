"""
EODHD API Client
Handles all API interactions with EODHD service
Optimized with httpx (HTTP/2) and orjson for faster performance
"""

import httpx
import orjson
import time
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import logging

from config.eodhd_config import (
    EODHD_API_KEY,
    EODHD_BASE_URL,
    EOD_ENDPOINT,
    INTRADAY_ENDPOINT,
    FUNDAMENTALS_ENDPOINT,
    DIVIDENDS_ENDPOINT,
    SPLITS_ENDPOINT,
    EARNINGS_CALENDAR_ENDPOINT,
    REQUEST_DELAY,
    MAX_RETRIES,
    RETRY_DELAY
)

logger = logging.getLogger(__name__)


class EODHDClient:
    """Client for EODHD API with HTTP/2 and optimized JSON parsing"""
    
    def __init__(self, api_key: str = EODHD_API_KEY):
        self.api_key = api_key
        self.base_url = EODHD_BASE_URL
        
        # Create httpx client with HTTP/2 and connection pooling
        self.client = httpx.Client(
            http2=True,  # Enable HTTP/2 for multiplexing
            limits=httpx.Limits(
                max_connections=20,
                max_keepalive_connections=10
            ),
            timeout=httpx.Timeout(30.0),
            # Retry configuration
            transport=httpx.HTTPTransport(retries=MAX_RETRIES)
        )
        
        self.last_request_time = 0
        
    def _rate_limit(self):
        """Enforce rate limiting"""
        elapsed = time.time() - self.last_request_time
        if elapsed < REQUEST_DELAY:
            time.sleep(REQUEST_DELAY - elapsed)
        self.last_request_time = time.time()
    
    def _make_request(self, url: str, params: Dict) -> Optional[Dict]:
        """Make HTTP request with retry logic and optimized JSON parsing"""
        params['api_token'] = self.api_key
        params['fmt'] = 'json'
        
        for attempt in range(MAX_RETRIES):
            try:
                self._rate_limit()
                response = self.client.get(url, params=params)
                response.raise_for_status()
                # Use orjson for 2-3x faster JSON parsing
                return orjson.loads(response.content)
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 422:
                    logger.warning(f"Invalid request (422): {url}")
                    return None
                logger.error(f"HTTP error (attempt {attempt + 1}/{MAX_RETRIES}): {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)
            except Exception as e:
                logger.error(f"Request error (attempt {attempt + 1}/{MAX_RETRIES}): {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)
        
        return None
    
    def get_eod_data(self, symbol: str, period: str = 'd', 
                     from_date: Optional[datetime] = None,
                     to_date: Optional[datetime] = None) -> Optional[List[Dict]]:
        """
        Fetch EOD (End of Day) data
        
        Args:
            symbol: Stock symbol (e.g., 'BBCA.JK')
            period: 'd' (daily), 'w' (weekly), 'm' (monthly)
            from_date: Start date (optional)
            to_date: End date (optional)
        
        Returns:
            List of OHLCV data dictionaries
        """
        url = f"{self.base_url}{EOD_ENDPOINT.format(symbol=symbol)}"
        params = {'period': period}
        
        if from_date:
            params['from'] = from_date.strftime('%Y-%m-%d')
        if to_date:
            params['to'] = to_date.strftime('%Y-%m-%d')
        
        logger.info(f"Fetching EOD data for {symbol} (period={period})")
        return self._make_request(url, params)
    
    def get_intraday_data(self, symbol: str, interval: str = '5m',
                          from_timestamp: Optional[int] = None,
                          to_timestamp: Optional[int] = None) -> Optional[List[Dict]]:
        """
        Fetch intraday data
        
        Args:
            symbol: Stock symbol (e.g., 'BBCA.JK')
            interval: '5m', '15m', '30m', '1h'
            from_timestamp: Start timestamp (Unix)
            to_timestamp: End timestamp (Unix)
        
        Returns:
            List of OHLCV data dictionaries
        """
        url = f"{self.base_url}{INTRADAY_ENDPOINT.format(symbol=symbol)}"
        params = {'interval': interval}
        
        if from_timestamp:
            params['from'] = from_timestamp
        if to_timestamp:
            params['to'] = to_timestamp
        
        logger.info(f"Fetching intraday data for {symbol} (interval={interval})")
        return self._make_request(url, params)
    
    def get_fundamentals(self, symbol: str) -> Optional[Dict]:
        """
        Fetch fundamental data
        
        Args:
            symbol: Stock symbol (e.g., 'BBCA.JK')
        
        Returns:
            Dictionary with fundamental data
        """
        url = f"{self.base_url}{FUNDAMENTALS_ENDPOINT.format(symbol=symbol)}"
        params = {}
        
        logger.info(f"Fetching fundamentals for {symbol}")
        return self._make_request(url, params)
    
    def get_dividends(self, symbol: str,
                     from_date: Optional[datetime] = None,
                     to_date: Optional[datetime] = None) -> Optional[List[Dict]]:
        """
        Fetch dividend history
        
        Args:
            symbol: Stock symbol (e.g., 'BBCA.JK')
            from_date: Start date (optional)
            to_date: End date (optional)
        
        Returns:
            List of dividend dictionaries
        """
        url = f"{self.base_url}{DIVIDENDS_ENDPOINT.format(symbol=symbol)}"
        params = {}
        
        if from_date:
            params['from'] = from_date.strftime('%Y-%m-%d')
        if to_date:
            params['to'] = to_date.strftime('%Y-%m-%d')
        
        logger.info(f"Fetching dividends for {symbol}")
        return self._make_request(url, params)
    
    def get_splits(self, symbol: str,
                   from_date: Optional[datetime] = None,
                   to_date: Optional[datetime] = None) -> Optional[List[Dict]]:
        """
        Fetch stock split history
        
        Args:
            symbol: Stock symbol (e.g., 'BBCA.JK')
            from_date: Start date (optional)
            to_date: End date (optional)
        
        Returns:
            List of split dictionaries
        """
        url = f"{self.base_url}{SPLITS_ENDPOINT.format(symbol=symbol)}"
        params = {}
        
        if from_date:
            params['from'] = from_date.strftime('%Y-%m-%d')
        if to_date:
            params['to'] = to_date.strftime('%Y-%m-%d')
        
        logger.info(f"Fetching splits for {symbol}")
        return self._make_request(url, params)
    
    def get_stock_metadata(self, symbol: str) -> Optional[Dict]:
        """
        Get stock metadata (name, exchange, currency, etc.)
        
        Args:
            symbol: Stock symbol (e.g., 'BBCA.JK')
        
        Returns:
            Dict with stock metadata or None
        """
        try:
            logger.info(f"Fetching metadata for {symbol}")
            url = f"{self.base_url}/search/{symbol}"
            params = {}
            
            data = self._make_request(url, params)
            
            if data and isinstance(data, list) and len(data) > 0:
                # Return first match (should be exact match)
                return data[0]
            
            return None
            
        except Exception as e:
            logger.error(f"Failed to fetch metadata for {symbol}: {e}")
            return None
    
    def get_exchange_symbols(self, exchange: str = 'JK') -> Optional[List[Dict]]:
        """
        Get all symbols for an exchange
        
        Args:
            exchange: Exchange code (default: 'JK' for Jakarta)
        
        Returns:
            List of dicts with symbol information or None
        """
        try:
            logger.info(f"Fetching symbols for exchange {exchange}")
            url = f"{self.base_url}/exchange-symbol-list/{exchange}"
            params = {}
            
            data = self._make_request(url, params)
            
            if data and isinstance(data, list):
                return data
            
            return None
            
        except Exception as e:
            logger.error(f"Failed to fetch exchange symbols for {exchange}: {e}")
            return None
    
    def get_earnings_calendar(self, symbols: Optional[List[str]] = None,
                             from_date: Optional[datetime] = None,
                             to_date: Optional[datetime] = None) -> Optional[Dict]:
        """
        Fetch earnings calendar
        
        Args:
            symbols: List of symbols (optional)
            from_date: Start date (optional)
            to_date: End date (optional)
        
        Returns:
            Dictionary with earnings calendar
        """
        url = f"{self.base_url}{EARNINGS_CALENDAR_ENDPOINT}"
        params = {}
        
        if symbols:
            params['symbols'] = ','.join(symbols)
        if from_date:
            params['from'] = from_date.strftime('%Y-%m-%d')
        if to_date:
            params['to'] = to_date.strftime('%Y-%m-%d')
        
        logger.info(f"Fetching earnings calendar")
        return self._make_request(url, params)
    
    def close(self):
        """Close httpx client and cleanup resources"""
        if hasattr(self, 'client'):
            self.client.close()
    
    def __del__(self):
        """Cleanup on garbage collection"""
        self.close()
