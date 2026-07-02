# EODHD API Configuration

import os
from dotenv import load_dotenv

load_dotenv()

# API Configuration
EODHD_API_KEY = os.getenv('EODHD_API_KEY', '')
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

# Bulk API — exchange-wide last-day EOD/dividends/splits in ONE request (costs 100 API calls).
# Replaces per-symbol EOD/dividend/split fetching for daily updates (far cheaper).
BULK_ENDPOINT = '/eod-bulk-last-day/{exchange}'
EXCHANGE_CODE = 'JK'  # Jakarta / IDX — VERIFY with a test bulk call before relying on it

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
INTRADAY_INTERVALS = ['5m', '15m', '1h']  # 5m re-added as base for dollar bars; 30m dropped; 4h derived from 1h via utils/aggregate_4h
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
