"""
4H Session Candle Aggregator for JK (IDX) Exchange

Aggregates 1h candles into session-aware 4h candles, following the
TradingView/Bloomberg approach:

  Session 1 (AM): 09:00-11:59 WIB  (02:00-04:59 UTC)  → timestamp 02:00 UTC
  Session 2 (PM): 13:00-16:00 WIB  (06:00-09:00 UTC)  → timestamp 06:00 UTC

  PM session includes the 16:00 WIB closing auction (09:00 UTC),
  matching TradingView's implementation.

Rules:
  - open  = first(open)   of the session's 1h candles
  - high  = max(high)     across all session candles
  - low   = min(low)      across all session candles
  - close = last(close)   of the session's 1h candles
  - volume = sum(volume)  across all session candles
  - Includes closing auction (16:00 WIB / 09:00 UTC) in PM session
  - Respects lunch break gap (12:00-12:59 WIB / 05:00 UTC)
  - Stored with interval='4h' in eodhd_stock_data
"""

import psycopg2
import logging
import time
from datetime import datetime
from typing import Optional

from config.db_config import PG_CONNECTION_STRING, TABLE_STOCK_DATA

logger = logging.getLogger(__name__)

# JK Exchange session boundaries (in UTC hours)
# Session 1 (AM): 09:00-11:59 WIB = 02:00-04:59 UTC → hours 2, 3, 4
# Session 2 (PM): 13:00-16:00 WIB = 06:00-09:00 UTC → hours 6, 7, 8, 9
#   Includes 16:00 WIB closing auction (hour 9 UTC) per TradingView
SESSION_1_HOURS = (2, 3, 4)          # Morning session (UTC)
SESSION_2_HOURS = (6, 7, 8, 9)      # Afternoon session + closing auction (UTC)
SESSION_1_START_UTC = 2              # 09:00 WIB
SESSION_2_START_UTC = 6              # 13:00 WIB


def aggregate_4h_candles(
    since: Optional[str] = None,
    symbols: Optional[list] = None,
    dry_run: bool = False,
) -> dict:
    """
    Aggregate 1h candles into 4h session candles and insert into eodhd_stock_data.
    
    Args:
        since: Only process 1h data from this date onwards (YYYY-MM-DD).
               If None, processes all historical data.
        symbols: List of specific symbols to process. If None, all symbols.
        dry_run: If True, only show what would be inserted without writing.
    
    Returns:
        Dict with stats: total_candles, symbols_processed, elapsed_seconds
    """
    start = time.time()
    
    conn = psycopg2.connect(PG_CONNECTION_STRING)
    conn.autocommit = True
    cur = conn.cursor()
    
    # Build WHERE clause for source 1h data
    conditions = [
        "interval = '1h'",
        # Only include trading session hours (incl. 16:00 WIB closing auction)
        f"(hour(timestamp) IN (2, 3, 4, 6, 7, 8, 9))",
        # Exclude placeholder candles with NULL OHLC (illiquid stocks)
        "open IS NOT NULL",
    ]
    
    if since:
        conditions.append(f"timestamp >= '{since}'")
    
    if symbols:
        symbol_list = ",".join(f"'{s}'" for s in symbols)
        conditions.append(f"symbol IN ({symbol_list})")
    
    where = " AND ".join(conditions)
    
    # Aggregate 1h → 4h using session assignment
    # hour 2,3,4 → session timestamp = date + 02:00 UTC (09:00 WIB)
    # hour 6,7,8,9 → session timestamp = date + 06:00 UTC (13:00 WIB)
    query = f"""
        SELECT 
            symbol,
            CASE 
                WHEN hour(timestamp) IN (2, 3, 4) 
                    THEN dateadd('h', 2, date_trunc('day', timestamp))
                WHEN hour(timestamp) IN (6, 7, 8, 9) 
                    THEN dateadd('h', 6, date_trunc('day', timestamp))
            END as session_ts,
            first(open) as open,
            max(high) as high,
            min(low) as low,
            last(close) as close,
            last(adjusted_close) as adjusted_close,
            sum(volume) as volume,
            '4h' as interval,
            0 as gmtoffset,
            'aggregated' as source
        FROM {TABLE_STOCK_DATA}
        WHERE {where}
        GROUP BY symbol, 
            CASE 
                WHEN hour(timestamp) IN (2, 3, 4) 
                    THEN dateadd('h', 2, date_trunc('day', timestamp))
                WHEN hour(timestamp) IN (6, 7, 8, 9) 
                    THEN dateadd('h', 6, date_trunc('day', timestamp))
            END
        ORDER BY symbol, session_ts
    """
    
    logger.info("Querying 1h data for 4h aggregation...")
    cur.execute(query)
    rows = cur.fetchall()
    
    if not rows:
        logger.warning("No 1h data found for aggregation")
        conn.close()
        return {"total_candles": 0, "symbols_processed": 0, "elapsed_seconds": 0}
    
    total_candles = len(rows)
    symbols_seen = set()
    
    if dry_run:
        logger.info(f"DRY RUN: Would insert {total_candles:,} 4h candles")
        for r in rows[:10]:
            symbols_seen.add(r[0])
            o = f"{r[2]:.2f}" if r[2] is not None else "N/A"
            h = f"{r[3]:.2f}" if r[3] is not None else "N/A"
            l = f"{r[4]:.2f}" if r[4] is not None else "N/A"
            c = f"{r[5]:.2f}" if r[5] is not None else "N/A"
            v = f"{r[7]:,.0f}" if r[7] is not None else "N/A"
            logger.info(f"  {r[0]} {r[1]} O={o} H={h} L={l} C={c} V={v}")
        conn.close()
        elapsed = time.time() - start
        return {
            "total_candles": total_candles,
            "symbols_processed": len(set(r[0] for r in rows)),
            "elapsed_seconds": round(elapsed, 1),
        }
    
    # Insert in batches using INSERT ... SELECT for efficiency
    # Since dedup is enabled, duplicates will be automatically resolved
    logger.info(f"Inserting {total_candles:,} 4h candles into {TABLE_STOCK_DATA}...")
    
    insert_query = f"""
        INSERT INTO {TABLE_STOCK_DATA} 
            (symbol, interval, timestamp, open, high, low, close, adjusted_close, volume, gmtoffset, source, created_at)
        SELECT 
            symbol,
            '4h' as interval,
            CASE 
                WHEN hour(timestamp) IN (2, 3, 4) 
                    THEN dateadd('h', 2, date_trunc('day', timestamp))
                WHEN hour(timestamp) IN (6, 7, 8, 9) 
                    THEN dateadd('h', 6, date_trunc('day', timestamp))
            END as session_ts,
            first(open) as open,
            max(high) as high,
            min(low) as low,
            last(close) as close,
            last(adjusted_close) as adjusted_close,
            sum(volume) as volume,
            0 as gmtoffset,
            'aggregated' as source,
            now() as created_at
        FROM {TABLE_STOCK_DATA}
        WHERE {where}
        GROUP BY symbol, 
            CASE 
                WHEN hour(timestamp) IN (2, 3, 4) 
                    THEN dateadd('h', 2, date_trunc('day', timestamp))
                WHEN hour(timestamp) IN (6, 7, 8, 9) 
                    THEN dateadd('h', 6, date_trunc('day', timestamp))
            END
    """
    
    cur.execute(insert_query)
    
    elapsed = time.time() - start
    symbols_count = len(set(r[0] for r in rows))
    
    logger.info(
        f"4h aggregation complete: {total_candles:,} candles for "
        f"{symbols_count} symbols in {elapsed:.1f}s"
    )
    
    conn.close()
    
    return {
        "total_candles": total_candles,
        "symbols_processed": symbols_count,
        "elapsed_seconds": round(elapsed, 1),
    }


def validate_4h_candles(symbol: str = "ASII.JK", days: int = 5) -> None:
    """
    Print a comparison of 1h source data vs 4h aggregated candles for validation.
    """
    conn = psycopg2.connect(PG_CONNECTION_STRING)
    cur = conn.cursor()
    
    print(f"\n=== 4h Candle Validation: {symbol} (last {days} days) ===\n")
    
    # Get 1h source candles (only trading hours)
    cur.execute(f"""
        SELECT timestamp, open, high, low, close, volume
        FROM {TABLE_STOCK_DATA}
        WHERE symbol = '{symbol}' 
          AND interval = '1h' 
          AND hour(timestamp) IN (2, 3, 4, 6, 7, 8, 9)
          AND timestamp > dateadd('d', -{days}, now())
        ORDER BY timestamp
    """)
    h1_rows = cur.fetchall()
    
    # Get 4h candles
    cur.execute(f"""
        SELECT timestamp, open, high, low, close, volume
        FROM {TABLE_STOCK_DATA}
        WHERE symbol = '{symbol}' 
          AND interval = '4h' 
          AND timestamp > dateadd('d', -{days}, now())
        ORDER BY timestamp
    """)
    h4_rows = cur.fetchall()
    
    print("Source 1h candles (trading hours only, WIB):")
    print(f"  {'Time WIB':<20} {'Open':>8} {'High':>8} {'Low':>8} {'Close':>8} {'Volume':>12}")
    print(f"  {'-'*20} {'-'*8} {'-'*8} {'-'*8} {'-'*8} {'-'*12}")
    
    for r in h1_rows:
        wib_hour = r[0].hour + 7
        wib_str = r[0].strftime('%Y-%m-%d') + f" {wib_hour:02d}:00"
        session = "AM" if r[0].hour in SESSION_1_HOURS else "PM"
        vol = f"{r[5]:>12,}" if r[5] else f"{'N/A':>12}"
        print(f"  {wib_str:<20} {r[1]:>8.0f} {r[2]:>8.0f} {r[3]:>8.0f} {r[4]:>8.0f} {vol}  [{session}]")
    
    print(f"\nAggregated 4h candles:")
    print(f"  {'Time WIB':<20} {'Open':>8} {'High':>8} {'Low':>8} {'Close':>8} {'Volume':>12}")
    print(f"  {'-'*20} {'-'*8} {'-'*8} {'-'*8} {'-'*8} {'-'*12}")
    
    for r in h4_rows:
        wib_hour = r[0].hour + 7
        wib_str = r[0].strftime('%Y-%m-%d') + f" {wib_hour:02d}:00"
        session = "AM" if r[0].hour == SESSION_1_START_UTC else "PM"
        vol = f"{r[5]:>12,}" if r[5] else f"{'N/A':>12}"
        print(f"  {wib_str:<20} {r[1]:>8.0f} {r[2]:>8.0f} {r[3]:>8.0f} {r[4]:>8.0f} {vol}  [{session}]")
    
    print(f"\nSummary: {len(h1_rows)} 1h candles → {len(h4_rows)} 4h candles")
    
    conn.close()


if __name__ == "__main__":
    import argparse
    
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    
    parser = argparse.ArgumentParser(description="Aggregate 1h → 4h session candles")
    parser.add_argument("--since", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--symbol", help="Single symbol to process")
    parser.add_argument("--dry-run", action="store_true", help="Preview without inserting")
    parser.add_argument("--validate", help="Validate 4h candles for a symbol")
    
    args = parser.parse_args()
    
    if args.validate:
        validate_4h_candles(symbol=args.validate)
    else:
        symbols = [args.symbol] if args.symbol else None
        result = aggregate_4h_candles(
            since=args.since,
            symbols=symbols,
            dry_run=args.dry_run,
        )
        print(f"\nResult: {result}")
