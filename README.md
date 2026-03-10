# EODHD Data Collector

Comprehensive stock data collector using EODHD API for Indonesian Syariah stocks.

## Features

- **5 Data Types**: Price, Fundamentals, Corporate Actions, Calendar Events, Technical Indicators
- **8 Price Intervals**: 5m, 15m, 30m, 1h, 1d, 1w, 1M
- **600 Days** of intraday history
- **Full EOD** history (45+ years)
- **Automatic NULL filtering** (market breaks)
- **Rate limiting** and retry logic

## Project Structure

```
eodhd_collector/
├── api/
│   └── eodhd_client.py         # EODHD API client
├── collectors/
│   ├── price_collector.py      # Price data collector
│   ├── fundamental_collector.py # Fundamentals collector
│   ├── action_collector.py     # Corporate actions collector
│   └── calendar_collector.py   # Calendar events collector
├── db/
│   └── schemas.sql             # QuestDB table schemas
├── config/
│   ├── eodhd_config.py         # API configuration
│   ├── db_config.py            # Database configuration
│   └── test_stocks.txt         # Test stock list (50 stocks)
├── utils/
│   ├── data_filter.py          # NULL filtering & validation
│   └── logger.py               # Logging setup
├── .env                        # Environment variables
├── requirements.txt            # Python dependencies
└── test_api.py                 # API connection test

## Quick Start

### 1. Install Dependencies

```bash
cd /home/djp/eodhd_collector
pip install -r requirements.txt
```

### 2. Configure Environment

Edit `.env` file with your settings (already pre-configured).

### 3. Create Database Tables

Connect to QuestDB and run:
```bash
psql -h localhost -p 8812 -U admin -d qdb -f db/schemas.sql
```

Or use QuestDB web console at http://localhost:9000

### 4. Test API Connection

```bash
python test_api.py
```

This will test:
- EOD data fetching
- Intraday data fetching
- Fundamentals fetching
- Dividends fetching

### 5. Run Data Collection

```bash
# Collect 50 test stocks
python main.py --stocks config/test_stocks.txt

# Collect all 651 stocks
python main.py --stocks ../questDBDocker/config/syariah_stocks.txt
```

## Database Tables

All tables use `eodhd_` prefix to avoid conflicts with existing tables:

1. **eodhd_stock_data** - Price data (all intervals)
2. **eodhd_fundamentals** - Fundamental metrics
3. **eodhd_corporate_actions** - Dividends & splits
4. **eodhd_calendar_events** - Upcoming events
5. **eodhd_metadata** - Collection status

## Configuration

### API Settings (`config/eodhd_config.py`)
- API key
- Rate limits
- Intervals
- Historical depth

### Database Settings (`config/db_config.py`)
- QuestDB connection
- Table names
- Batch sizes

## Data Collection

### Initial Collection (50 stocks)
- Time: ~1 minute
- Storage: ~263 MB
- Records: ~2.7M

### Full Collection (651 stocks)
- Time: ~13 minutes
- Storage: ~3.39 GB
- Records: ~35M

### Daily Updates
- Time: ~2 minutes
- Storage: ~6.5 MB/day
- Records: ~130K/day

## Testing

Start with 50 stocks from `config/test_stocks.txt` to:
- Verify API connection
- Test data quality
- Check database storage
- Validate NULL filtering

Then expand to all 651 stocks.

## Monitoring

Check logs at: `logs/eodhd_collector.log`

Query collection status:
```sql
SELECT symbol, last_price_update, total_price_records 
FROM eodhd_metadata 
ORDER BY symbol;
```

## Support

For issues or questions, check:
- EODHD API docs: https://eodhistoricaldata.com/financial-apis/
- QuestDB docs: https://questdb.io/docs/

## License

Private use only.
