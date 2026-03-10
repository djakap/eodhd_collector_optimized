# Database Configuration for EODHD Collector

import os
from dotenv import load_dotenv

load_dotenv()

# QuestDB Configuration (Same instance as existing project)
QUESTDB_HOST = os.getenv('QUESTDB_HOST', 'localhost')
QUESTDB_HTTP_PORT = int(os.getenv('QUESTDB_HTTP_PORT', 9000))
QUESTDB_PG_PORT = int(os.getenv('QUESTDB_PG_PORT', 8812))
QUESTDB_INFLUX_PORT = int(os.getenv('QUESTDB_INFLUX_PORT', 9009))

# Database credentials
QUESTDB_USER = os.getenv('QUESTDB_USER', 'admin')
QUESTDB_PASSWORD = os.getenv('QUESTDB_PASSWORD', 'quest')
QUESTDB_DATABASE = os.getenv('QUESTDB_DATABASE', 'qdb')

# Connection strings
HTTP_URL = f"http://{QUESTDB_HOST}:{QUESTDB_HTTP_PORT}"
PG_CONNECTION_STRING = f"postgresql://{QUESTDB_USER}:{QUESTDB_PASSWORD}@{QUESTDB_HOST}:{QUESTDB_PG_PORT}/{QUESTDB_DATABASE}"

# Table names (prefixed with 'eodhd_' to avoid conflicts)
TABLE_STOCK_DATA = 'eodhd_stock_data'
TABLE_FUNDAMENTALS = 'eodhd_fundamentals'
TABLE_CORPORATE_ACTIONS = 'eodhd_corporate_actions'
TABLE_CALENDAR_EVENTS = 'eodhd_calendar_events'
TABLE_METADATA = 'eodhd_metadata'
TABLE_STOCK_METADATA = 'eodhd_stock_metadata'  # For update mode tracking

# Batch insert settings (optimized for large datasets)
BATCH_INSERT_SIZE = 2000  # Reduced to prevent QuestDB crashes
INSERT_TIMEOUT = 60  # seconds (increased for larger batches)

# Connection pool settings
POOL_SIZE = 5
MAX_OVERFLOW = 10
POOL_TIMEOUT = 30
