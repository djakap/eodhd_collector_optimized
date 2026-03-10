"""
Corporate Actions Collector
Collects dividends and stock splits
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime
from typing import List, Dict
import logging

from api.eodhd_client import EODHDClient
from db.questdb_client import QuestDBClient

logger = logging.getLogger(__name__)


class ActionCollector:
    """Collects corporate actions (dividends & splits)"""
    
    def __init__(self):
        self.api_client = EODHDClient()
        self.db_client = QuestDBClient()
        self.db_client.connect()
    
    def close(self):
        """Close database connection"""
        self.db_client.close()
    
    def collect_dividends(self, symbol: str) -> int:
        """
        Collect dividend history
        
        Args:
            symbol: Stock symbol (e.g., 'BBCA.JK')
        
        Returns:
            Number of records inserted
        """
        logger.info(f"Collecting dividends for {symbol}")
        
        # Fetch all dividend history
        data = self.api_client.get_dividends(symbol)
        
        if not data:
            logger.info(f"No dividends for {symbol}")
            return 0
        
        # Convert to database format
        records = []
        for item in data:
            date_str = item.get('date')
            if not date_str:
                continue
            
            action_date = datetime.strptime(date_str, '%Y-%m-%d').date()
            
            # Parse payment date
            payment_date = None
            if item.get('paymentDate'):
                try:
                    payment_date = datetime.strptime(item['paymentDate'], '%Y-%m-%d').date()
                except:
                    pass
            
            # Parse record date
            record_date = None
            if item.get('recordDate'):
                try:
                    record_date = datetime.strptime(item['recordDate'], '%Y-%m-%d').date()
                except:
                    pass
            
            # Parse declaration date
            declaration_date = None
            if item.get('declarationDate'):
                try:
                    declaration_date = datetime.strptime(item['declarationDate'], '%Y-%m-%d').date()
                except:
                    pass
            
            records.append({
                'symbol': symbol,
                'action_type': 'dividend',
                'action_date': action_date,
                'dividend_amount': item.get('value'),
                'dividend_currency': item.get('currency', 'IDR'),
                'payment_date': payment_date,
                'record_date': record_date,
                'declaration_date': declaration_date,
                'dividend_type': item.get('period'),
                'split_ratio': None,
                'split_from': None,
                'split_to': None
            })
        
        if records:
            self.db_client.insert_corporate_actions(records)
            logger.info(f"Inserted {len(records)} dividend records for {symbol}")
        
        return len(records)
    
    def collect_splits(self, symbol: str) -> int:
        """
        Collect stock split history
        
        Args:
            symbol: Stock symbol (e.g., 'BBCA.JK')
        
        Returns:
            Number of records inserted
        """
        logger.info(f"Collecting splits for {symbol}")
        
        # Fetch all split history
        data = self.api_client.get_splits(symbol)
        
        if not data:
            logger.info(f"No splits for {symbol}")
            return 0
        
        # Convert to database format
        records = []
        for item in data:
            date_str = item.get('date')
            if not date_str:
                continue
            
            action_date = datetime.strptime(date_str, '%Y-%m-%d').date()
            
            # Parse split ratio (e.g., "2/1")
            split_ratio = item.get('split', '')
            split_from = None
            split_to = None
            
            if '/' in split_ratio:
                parts = split_ratio.split('/')
                try:
                    split_from = int(parts[0])
                    split_to = int(parts[1])
                except:
                    pass
            
            records.append({
                'symbol': symbol,
                'action_type': 'split',
                'action_date': action_date,
                'dividend_amount': None,
                'dividend_currency': None,
                'payment_date': None,
                'record_date': None,
                'declaration_date': None,
                'dividend_type': None,
                'split_ratio': split_ratio,
                'split_from': split_from,
                'split_to': split_to
            })
        
        if records:
            self.db_client.insert_corporate_actions(records)
            logger.info(f"Inserted {len(records)} split records for {symbol}")
        
        return len(records)
    
    def collect_all_actions(self, symbol: str) -> Dict:
        """
        Collect all corporate actions (dividends + splits)
        
        Args:
            symbol: Stock symbol (e.g., 'BBCA.JK')
        
        Returns:
            Dictionary with collection stats
        """
        logger.info(f"Starting corporate actions collection for {symbol}")
        
        stats = {
            'symbol': symbol,
            'dividends': 0,
            'splits': 0,
            'total': 0,
            'success': False,
            'error': None
        }
        
        try:
            # Collect dividends
            div_count = self.collect_dividends(symbol)
            stats['dividends'] = div_count
            
            # Collect splits
            split_count = self.collect_splits(symbol)
            stats['splits'] = split_count
            
            stats['total'] = div_count + split_count
            stats['success'] = True
            
            # Update metadata
            self.db_client.insert_or_update_metadata(symbol, {
                'last_action_update': datetime.now(),
                'has_dividends': div_count > 0,
                'has_splits': split_count > 0,
                'total_dividends': div_count,
                'total_splits': split_count
            })
            
            logger.info(f"✅ Completed {symbol}: {div_count} dividends, {split_count} splits")
            
        except Exception as e:
            stats['error'] = str(e)
            logger.error(f"❌ Failed to collect actions for {symbol}: {e}")
        
        return stats


if __name__ == "__main__":
    # Test with single stock
    from utils.logger import setup_logging
    setup_logging()
    
    collector = ActionCollector()
    try:
        stats = collector.collect_all_actions('BBCA.JK')
        print(f"\nCollection Stats: {stats}")
    finally:
        collector.close()
