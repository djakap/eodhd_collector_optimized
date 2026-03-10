#!/usr/bin/env python
"""
Collect stock metadata for all stocks on JK exchange
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import argparse
from datetime import datetime
import logging

from collectors.metadata_collector import MetadataCollector
from utils.logger import setup_logging

logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description='Collect Stock Metadata')
    parser.add_argument('--exchange', default='JK', help='Exchange code (default: JK)')
    parser.add_argument('--symbol', help='Collect metadata for single symbol (e.g., BBCA.JK)')
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging()
    
    logger.info("="*70)
    logger.info("STOCK METADATA COLLECTION")
    logger.info("="*70)
    
    collector = MetadataCollector()
    
    try:
        if args.symbol:
            # Single symbol
            print(f"\n📊 Collecting metadata for {args.symbol}...")
            result = collector.collect_metadata(args.symbol)
            
            if result and result['success']:
                print(f"✅ Success: {result['name']} ({result['exchange']})")
            else:
                print(f"❌ Failed: {result.get('error', 'Unknown error')}")
        else:
            # Entire exchange
            print(f"\n📊 Collecting metadata for {args.exchange} exchange...")
            start_time = datetime.now()
            
            result = collector.collect_exchange_metadata(args.exchange)
            
            elapsed = (datetime.now() - start_time).total_seconds()
            
            if result['success']:
                print(f"\n✅ Collection Complete!")
                print(f"   Total symbols: {result['total']}")
                print(f"   Inserted: {result['inserted']}")
                print(f"   Time: {elapsed:.1f}s")
            else:
                print(f"\n❌ Collection Failed: {result.get('error', 'Unknown error')}")
    
    finally:
        collector.close()


if __name__ == "__main__":
    main()
