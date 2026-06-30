"""
Bulk Collector

Fetches last-day EOD prices, dividends, and splits for an ENTIRE exchange in
single requests via the EODHD Bulk API (eod-bulk-last-day). Each bulk request
costs 100 API calls but covers all ~650 IDX symbols, replacing per-symbol
fetching for daily updates.

NOTE: bulk returns only the LAST trading day (or the given `date`). It does not
provide weekly/monthly EOD — derive those from daily, or keep them per-symbol.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime
from typing import Optional
import logging

from api.eodhd_client import EODHDClient
from db.questdb_client import QuestDBClient
from config.eodhd_config import EXCHANGE_CODE

logger = logging.getLogger(__name__)


def _to_int(v):
    try:
        return int(v) if v is not None else None
    except (ValueError, TypeError):
        return None


def _to_float(v):
    try:
        return float(v) if v is not None else None
    except (ValueError, TypeError):
        return None


def _parse_date(s):
    if not s:
        return None
    try:
        return datetime.strptime(s, '%Y-%m-%d').date()
    except (ValueError, TypeError):
        return None


class BulkCollector:
    """Collects exchange-wide last-day EOD / dividends / splits via the Bulk API."""

    def __init__(self, exchange: str = EXCHANGE_CODE):
        self.exchange = exchange
        self.api_client = EODHDClient()
        self.db_client = QuestDBClient()
        self.db_client.connect()

    def close(self):
        try:
            self.db_client.close()
        finally:
            self.api_client.close()

    def _symbol(self, code: str) -> str:
        # bulk 'code' may or may not carry the exchange suffix; normalise to '<CODE>.<EXCHANGE>'
        return code if '.' in code else f"{code}.{self.exchange}"

    def collect_eod(self, date: Optional[str] = None) -> int:
        """Bulk last-day EOD (interval 'd') for the whole exchange -> eodhd_stock_data."""
        data = self.api_client.get_bulk_eod(self.exchange, date=date)
        if not data:
            logger.warning(f"No bulk EOD data for {self.exchange} (date={date or 'last'})")
            return 0

        records = []
        for item in data:
            code = item.get('code')
            date_str = item.get('date')
            if not code or not date_str:
                continue
            ts = _parse_date(date_str)
            if ts is None:
                continue
            ts = datetime(ts.year, ts.month, ts.day)
            records.append((
                self._symbol(code), 'd', ts,
                _to_float(item.get('open')), _to_float(item.get('high')),
                _to_float(item.get('low')), _to_float(item.get('close')),
                _to_float(item.get('adjusted_close')), _to_int(item.get('volume')),
                None, 'eod_bulk', datetime.now(),
            ))

        if records:
            self.db_client.insert_price_data(records)
        logger.info(f"Bulk EOD: {len(records)} daily bars for {self.exchange}")
        return len(records)

    def collect_dividends(self, date: Optional[str] = None) -> int:
        """Bulk last-day dividends for the whole exchange -> eodhd_corporate_actions."""
        data = self.api_client.get_bulk_eod(self.exchange, date=date, action_type='dividends')
        if not data:
            logger.info(f"No bulk dividends for {self.exchange} (date={date or 'last'})")
            return 0

        records = []
        for item in data:
            code = item.get('code')
            date_str = item.get('date') or item.get('ex_date') or item.get('exDate')
            value = item.get('value') if item.get('value') is not None else item.get('dividend')
            if not code or not date_str:
                continue
            action_date = _parse_date(date_str)
            if action_date is None:
                continue
            records.append({
                'symbol': self._symbol(code),
                'action_type': 'dividend',
                'action_date': action_date,
                'dividend_amount': _to_float(value),
                'dividend_currency': item.get('currency', 'IDR'),
                'payment_date': _parse_date(item.get('paymentDate')),
                'record_date': _parse_date(item.get('recordDate')),
                'declaration_date': _parse_date(item.get('declarationDate')),
                'dividend_type': item.get('period'),
                'split_ratio': None, 'split_from': None, 'split_to': None,
            })

        if records:
            self.db_client.insert_corporate_actions(records)
        logger.info(f"Bulk dividends: {len(records)} records for {self.exchange}")
        return len(records)

    def collect_splits(self, date: Optional[str] = None) -> int:
        """Bulk last-day splits for the whole exchange -> eodhd_corporate_actions."""
        data = self.api_client.get_bulk_eod(self.exchange, date=date, action_type='splits')
        if not data:
            logger.info(f"No bulk splits for {self.exchange} (date={date or 'last'})")
            return 0

        records = []
        for item in data:
            code = item.get('code')
            date_str = item.get('date') or item.get('split_date')
            split_ratio = item.get('split', '') or ''
            if not code or not date_str:
                continue
            action_date = _parse_date(date_str)
            if action_date is None:
                continue
            split_from = split_to = None
            if '/' in split_ratio:
                parts = split_ratio.split('/')
                try:
                    split_from = int(float(parts[0]))
                    split_to = int(float(parts[1]))
                except (ValueError, TypeError):
                    pass
            records.append({
                'symbol': self._symbol(code),
                'action_type': 'split',
                'action_date': action_date,
                'dividend_amount': None, 'dividend_currency': None,
                'payment_date': None, 'record_date': None, 'declaration_date': None,
                'dividend_type': None,
                'split_ratio': split_ratio, 'split_from': split_from, 'split_to': split_to,
            })

        if records:
            self.db_client.insert_corporate_actions(records)
        logger.info(f"Bulk splits: {len(records)} records for {self.exchange}")
        return len(records)

    def collect_all(self, date: Optional[str] = None) -> dict:
        return {
            'eod': self.collect_eod(date),
            'dividends': self.collect_dividends(date),
            'splits': self.collect_splits(date),
        }
