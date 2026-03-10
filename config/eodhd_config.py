# EODHD API Configuration

import os
from dotenv import load_dotenv

load_dotenv()

# API Configuration
EODHD_API_KEY = os.getenv('EODHD_API_KEY', '6976bf4be7ab42.10938432')
EODHD_BASE_URL = 'https://eodhistoricaldata.com/api'

# API Endpoints
EOD_ENDPOINT = '/eod/{symbol}'
INTRADAY_ENDPOINT = '/intraday/{symbol}'
FUNDAMENTALS_ENDPOINT = '/fundamentals/{symbol}'
DIVIDENDS_ENDPOINT = '/div/{symbol}'
SPLITS_ENDPOINT = '/splits/{symbol}'
EARNINGS_CALENDAR_ENDPOINT = '/calendar/earnings'
IPO_CALENDAR_ENDPOINT = '/calendar/ipos'
SPLITS_CALENDAR_ENDPOINT = '/calendar/splits'

# Rate Limiting (All World Extended plan)
REQUESTS_PER_SECOND = 10
REQUESTS_PER_MINUTE = 600
REQUESTS_PER_DAY = 100000

# Delay between requests (seconds)
REQUEST_DELAY = 0.1  # 100ms = 10 req/sec

# Symbol Configuration
EXCHANGE_SUFFIX = {
    'JK': '.JK',  # Jakarta Stock Exchange
    'US': '.US',  # US Exchanges
}

# Data Collection Settings
INTRADAY_INTERVALS = ['5m', '15m', '30m', '1h']  # Skip 1m (not available for IDX)
EOD_PERIODS = ['d', 'w', 'm']  # Daily, Weekly, Monthly

# Historical Data Limits
INTRADAY_MAX_DAYS = 600  # Maximum available for Indonesian stocks
EOD_MAX_YEARS = 50       # Fetch all available

# Batch Processing
BATCH_SIZE = 10
BATCH_DELAY = 1.0  # 1 second between batches

# Retry Configuration
MAX_RETRIES = 3
RETRY_DELAY = 2.0  # seconds

# Logging
LOG_LEVEL = 'INFO'
LOG_FILE = 'logs/eodhd_collector.log'
