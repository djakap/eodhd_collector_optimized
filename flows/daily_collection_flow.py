"""
Prefect Flow: Daily EODHD Data Collection (Medium Implementation)

Wraps existing main_ultrafast.py logic with:
- @task / @flow decorators for visibility & retry
- Task-level error isolation (one stock failing won't stop the rest)
- Automatic retries (3x with backoff)
- Caching for metadata tasks
- Progress tracking in Prefect UI (localhost:4200)

UNCHANGED:
- ILP protocol inserts (10-100x faster)
- Batch size 5000 / connection pooling
- ThreadPoolExecutor parallel workers
- Skip validation optimization
- All collector logic (price_collector, action_collector, questdb_client)
"""

import sys
import os

# Ensure project root is on path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional

from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from prefect.cache_policies import INPUTS

from collectors.price_collector import PriceCollector
from collectors.action_collector import ActionCollector
from db.questdb_client import QuestDBClient
from config.eodhd_config import INTRADAY_INTERVALS
from utils.logger import setup_logging
from utils.aggregate_4h import aggregate_4h_candles

# ---------------------------------------------------------------------------
# TASKS — thin wrappers around existing collector logic
# ---------------------------------------------------------------------------

@task(name="Load stock list", retries=0)
def load_stocks(file_path: str) -> List[str]:
    """Load stock symbols from file — identical to main_ultrafast.load_stocks"""
    logger = get_run_logger()
    stocks = []
    with open(file_path, "r") as f:
        for line in f:
            stock = line.strip()
            if stock and not stock.startswith("#"):
                if not stock.endswith(".JK"):
                    stock = f"{stock}.JK"
                stocks.append(stock)
    logger.info(f"Loaded {len(stocks)} stocks from {file_path}")
    return stocks


@task(
    name="Collect stock data",
    retries=3,
    retry_delay_seconds=[30, 60, 120],   # exponential backoff
)
def collect_single_stock(
    symbol: str,
    collect_price: bool = True,
    collect_actions: bool = True,
    intraday_days: int = 120,
    skip_intraday: bool = False,
    skip_validation: bool = True,
    update_mode: bool = False,
    update_window: int = 7,
    skip_duplicate_check: bool = False,
) -> Dict:
    """
    Collect all data for a single stock.
    
    Each task invocation creates its own PriceCollector / ActionCollector
    (same pattern as the ThreadPoolExecutor workers in main_ultrafast.py).
    """
    logger = get_run_logger()
    stats = {
        "symbol": symbol,
        "price_stats": None,
        "action_stats": None,
        "success": False,
        "error": None,
    }

    price_collector = None
    action_collector = None

    try:
        if collect_price:
            price_collector = PriceCollector(
                skip_validation=skip_validation,
                update_mode=update_mode,
                update_window=update_window,
                symbols_to_preload=None,
                skip_duplicate_check=skip_duplicate_check,
            )

            if skip_intraday:
                price_stats = {
                    "eod_records": price_collector.collect_eod_data(symbol),
                    "intraday_records": 0,
                    "total_records": 0,
                }
                price_stats["total_records"] = price_stats["eod_records"]
            else:
                price_stats = price_collector.collect_all_intervals(
                    symbol, intraday_days=intraday_days
                )
            stats["price_stats"] = price_stats

        if collect_actions:
            action_collector = ActionCollector()
            action_stats = action_collector.collect_all_actions(symbol)
            stats["action_stats"] = action_stats

        stats["success"] = True
        logger.info(
            f"{symbol}: ✅ "
            f"{stats['price_stats']['total_records'] if stats['price_stats'] else 0} records"
        )

    except Exception as e:
        stats["error"] = str(e)
        logger.error(f"{symbol}: ❌ {e}")
        raise  # let Prefect retry

    finally:
        if price_collector:
            price_collector.close()
        if action_collector:
            action_collector.close()

    return stats


@task(name="Save collection stats", retries=0)
def save_stats(all_stats: List[Dict]) -> str:
    """Save collection statistics to file (same as main_ultrafast)"""
    logger = get_run_logger()
    stats_file = f"data/collection_stats_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    os.makedirs("data", exist_ok=True)
    with open(stats_file, "w") as f:
        for stat in all_stats:
            f.write(f"{stat}\n")
    logger.info(f"Stats saved to {stats_file}")
    return stats_file


@task(name="Print summary", retries=0)
def print_summary(all_stats: List[Dict], elapsed_seconds: float):
    """Print final collection summary"""
    logger = get_run_logger()
    successful = sum(1 for s in all_stats if s.get("success"))
    failed = len(all_stats) - successful

    total_records = sum(
        s["price_stats"]["total_records"]
        for s in all_stats
        if s.get("success") and s.get("price_stats")
    )
    total_dividends = sum(
        s["action_stats"]["dividends"]
        for s in all_stats
        if s.get("success") and s.get("action_stats")
    )
    total_splits = sum(
        s["action_stats"]["splits"]
        for s in all_stats
        if s.get("success") and s.get("action_stats")
    )

    summary = f"""
{'='*70}
🎉 COLLECTION COMPLETE (Prefect Flow)
{'='*70}
📊 Summary:
   Total stocks: {len(all_stats)}
   ✅ Successful: {successful}
   ❌ Failed: {failed}
   📈 Total price records: {total_records:,}
   💰 Total dividends: {total_dividends}
   📊 Total splits: {total_splits}
   ⏱️  Time elapsed: {elapsed_seconds:.1f}s ({elapsed_seconds/60:.1f} min)
   ⚡ Avg per stock: {elapsed_seconds/max(len(all_stats),1):.1f}s
{'='*70}
"""
    logger.info(summary)
    print(summary, flush=True)


# ---------------------------------------------------------------------------
# FLOWS — orchestration layer
# ---------------------------------------------------------------------------

@flow(
    name="Daily Collection",
    description="Collect EODHD price & corporate action data for IDX Syariah stocks",
    retries=0,
    timeout_seconds=14400,  # 4 hour max for full collection
)
def daily_collection_flow(
    stocks_file: str = "config/syariah_stocks.txt",
    collect_price: bool = True,
    collect_actions: bool = True,
    intraday_days: int = 120,
    skip_intraday: bool = False,
    skip_validation: bool = True,
    update_mode: bool = False,
    update_window: int = 7,
    skip_duplicate_check: bool = False,
    limit: Optional[int] = None,
):
    """
    Main daily collection flow.
    
    Submits each stock as an independent Prefect task so that:
    - Each stock shows up in the UI individually
    - Failed stocks retry independently (3x with backoff)
    - You get per-stock timing and error visibility
    
    Uses the same parameters as main_ultrafast.py CLI.
    """
    logger = get_run_logger()
    setup_logging()

    logger.info("=" * 70)
    logger.info("EODHD DATA COLLECTION — Prefect Flow")
    logger.info("=" * 70)

    # 1. Load stocks
    stocks = load_stocks(stocks_file)
    if limit:
        stocks = stocks[:limit]
        logger.info(f"Limited to {limit} stocks")

    logger.info(f"📊 Stocks: {len(stocks)}")
    logger.info(f"📈 Price: {collect_price} | Actions: {collect_actions}")
    logger.info(f"⚡ Skip validation: {skip_validation} | Update mode: {update_mode}")

    # 2. Process stocks in batches to avoid exhausting DB connection pool
    #    Each stock needs ~2 connections (price + actions), pool max = 20
    #    Batch size of 4 keeps QuestDB memory usage manageable for large datasets
    BATCH_SIZE = 4
    start_time = datetime.now()
    all_stats = []

    total_batches = (len(stocks) + BATCH_SIZE - 1) // BATCH_SIZE
    for batch_idx in range(total_batches):
        batch_start = batch_idx * BATCH_SIZE
        batch_end = min(batch_start + BATCH_SIZE, len(stocks))
        batch = stocks[batch_start:batch_end]

        logger.info(f"📦 Batch {batch_idx + 1}/{total_batches}: {batch[0]} ... {batch[-1]} ({len(batch)} stocks)")

        # Submit batch of tasks concurrently
        futures = []
        for symbol in batch:
            future = collect_single_stock.submit(
                symbol=symbol,
                collect_price=collect_price,
                collect_actions=collect_actions,
                intraday_days=intraday_days,
                skip_intraday=skip_intraday,
                skip_validation=skip_validation,
                update_mode=update_mode,
                update_window=update_window,
                skip_duplicate_check=skip_duplicate_check,
            )
            futures.append(future)

        # Wait for this batch to complete before starting next
        for j, future in enumerate(futures):
            try:
                result = future.result()
                all_stats.append(result)
            except Exception as e:
                sym = batch[j]
                logger.error(f"{sym}: exhausted retries — {e}")
                all_stats.append({
                    "symbol": sym,
                    "price_stats": None,
                    "action_stats": None,
                    "success": False,
                    "error": str(e),
                })

        # Progress update
        done = len(all_stats)
        successful = sum(1 for s in all_stats if s.get("success"))
        elapsed = (datetime.now() - start_time).total_seconds()
        logger.info(
            f"   ✅ {successful}/{done} done | "
            f"{elapsed:.0f}s elapsed | "
            f"ETA {elapsed / max(done,1) * (len(stocks) - done):.0f}s"
        )

    elapsed = (datetime.now() - start_time).total_seconds()

    # 3. Summary & stats
    print_summary(all_stats, elapsed)
    stats_file = save_stats(all_stats)

    # 4. Aggregate 4h session candles from 1h data
    if not skip_intraday:
        logger.info("Aggregating 4h session candles from 1h data...")
        try:
            # Only re-aggregate recent data (update_window + buffer)
            since_date = (datetime.now() - timedelta(days=max(update_window, 7) + 3)).strftime('%Y-%m-%d') if update_mode else None
            agg_result = aggregate_4h_candles(since=since_date)
            logger.info(
                f"4h aggregation: {agg_result['total_candles']:,} candles for "
                f"{agg_result['symbols_processed']} symbols in {agg_result['elapsed_seconds']}s"
            )
        except Exception as e:
            logger.error(f"4h aggregation failed: {e}")

    # Release the shared QuestDB connection pool so idle connections aren't
    # held open after the flow finishes (worker process is long-lived)
    QuestDBClient.close_pool()

    return {
        "total_stocks": len(stocks),
        "successful": sum(1 for s in all_stats if s.get("success")),
        "failed": sum(1 for s in all_stats if not s.get("success")),
        "elapsed_seconds": elapsed,
        "stats_file": stats_file,
    }


@flow(
    name="Update Collection",
    description="Incremental update — only fetch recent data for stale stocks",
    retries=0,
    timeout_seconds=7200,  # 2 hour max
)
def update_collection_flow(
    stocks_file: str = "config/syariah_stocks.txt",
    update_window: int = 7,
    skip_intraday: bool = True,
    limit: Optional[int] = None,
):
    """
    Quick incremental update flow.
    
    Equivalent to:
      python main_ultrafast.py --stocks config/syariah_stocks.txt \
        --update-mode --skip-intraday --skip-duplicate-check
    """
    return daily_collection_flow(
        stocks_file=stocks_file,
        collect_price=True,
        collect_actions=True,
        intraday_days=120,
        skip_intraday=skip_intraday,
        skip_validation=True,
        update_mode=True,
        update_window=update_window,
        skip_duplicate_check=True,
        limit=limit,
    )


@flow(
    name="Screener Refresh",
    description="Smart refresh — only fetch stale tickers to keep QuestDB fresh for screener",
    retries=0,
    timeout_seconds=3600,  # 1 hour max
)
def screener_refresh_flow(
    stocks_file: str = "config/syariah_stocks.txt",
    interval: str = "d",
    max_age_minutes: int = 1440,
    max_workers: int = 5,
    force: bool = False,
    update_window: int = 7,
    limit: Optional[int] = None,
):
    """
    Smart refresh flow for screener integration.
    
    Checks data freshness in QuestDB and only fetches data for tickers
    that are stale. Much faster than a full collection when most data
    is already up-to-date.
    
    Schedule: Run daily at 18:00 WIB (after IDX close + EODHD data delay)
              or on-demand before screener scans.
    
    Equivalent to:
      python run_screener_refresh.py --stocks config/syariah_stocks.txt
    """
    from collectors.screener_refresh import ScreenerRefresh
    
    logger = get_run_logger()
    setup_logging()
    
    logger.info("=" * 60)
    logger.info("📡 SCREENER SMART REFRESH — Prefect Flow")
    logger.info("=" * 60)
    
    # Load stocks
    stocks = load_stocks(stocks_file)
    if limit:
        stocks = stocks[:limit]
    
    logger.info(f"📊 Tickers: {len(stocks)}")
    logger.info(f"📅 Interval: {interval}")
    logger.info(f"⏱️  Max age: {max_age_minutes} minutes")
    logger.info(f"🔄 Force: {force}")
    
    # Run smart refresh
    refresher = ScreenerRefresh(
        max_age_minutes=max_age_minutes,
        max_workers=max_workers
    )
    
    result = refresher.refresh(
        stocks,
        intervals=[interval],
        force=force,
        update_window=update_window
    )
    
    logger.info(
        f"✅ Done: {result['refreshed']} refreshed, "
        f"{result['already_fresh']} fresh, "
        f"{result['failed']} failed, "
        f"{result['elapsed_s']}s"
    )

    # Release the shared QuestDB connection pool (worker is long-lived)
    QuestDBClient.close_pool()

    return result


# ---------------------------------------------------------------------------
# CLI entry point — run flows directly with: python flows/daily_collection_flow.py
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Prefect EODHD Collection Flow")
    parser.add_argument(
        "--stocks", default="config/syariah_stocks.txt", help="Path to stock list"
    )
    parser.add_argument("--limit", type=int, help="Limit number of stocks")
    parser.add_argument(
        "--skip-intraday", action="store_true", help="Skip intraday (EOD only)"
    )
    parser.add_argument(
        "--update-mode", action="store_true", help="Incremental update mode"
    )
    parser.add_argument(
        "--update-window", type=int, default=7, help="Days to re-fetch (default: 7)"
    )
    parser.add_argument(
        "--skip-duplicate-check", action="store_true", help="Skip duplicate detection"
    )
    parser.add_argument(
        "--enable-validation", action="store_true", help="Enable OHLC validation"
    )
    parser.add_argument(
        "--screener-refresh", action="store_true",
        help="Run screener smart refresh (only fetch stale tickers)"
    )
    parser.add_argument(
        "--interval", type=str, default="d",
        help="Interval for screener refresh (default: d)"
    )
    parser.add_argument(
        "--max-age", type=int, default=1440,
        help="Max age in minutes for screener refresh (default: 1440)"
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Force refresh all tickers (screener mode)"
    )

    args = parser.parse_args()

    if args.screener_refresh:
        screener_refresh_flow(
            stocks_file=args.stocks,
            interval=args.interval,
            max_age_minutes=args.max_age,
            force=args.force,
            update_window=args.update_window,
            limit=args.limit,
        )
    elif args.update_mode:
        update_collection_flow(
            stocks_file=args.stocks,
            update_window=args.update_window,
            skip_intraday=args.skip_intraday,
            limit=args.limit,
        )
    else:
        daily_collection_flow(
            stocks_file=args.stocks,
            collect_price=True,
            collect_actions=True,
            intraday_days=120,
            skip_intraday=args.skip_intraday,
            skip_validation=not args.enable_validation,
            update_mode=False,
            update_window=args.update_window,
            skip_duplicate_check=args.skip_duplicate_check,
            limit=args.limit,
        )

