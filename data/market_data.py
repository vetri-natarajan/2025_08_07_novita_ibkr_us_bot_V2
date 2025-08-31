"""
data/market_data.py

Purpose:
- Provide a simple MarketData class to fetch historical bars from IB.
"""
import asyncio
import pandas as pd
import numpy as np
from datetime import datetime, timezone
from ib_async import IB, Stock, util
import logging
from collections import defaultdict
from typing import Optional

logger = logging.getLogger(__name__)



class MarketData:
    def __init__(self, ib: IB):
        self.ib = ib
        self.cache = defaultdict(dict)
        self.tickers = {}

    def create_stock_contract(self, symbol: str, exchange, currency):
        return Stock(symbol, exchange, currency)

    def subscribe(self, contract, timeframe='1 min'):
        pass

    def get_bars(self, contract, timeframe='1 min', n_bars=100) -> Optional[pd.DataFrame]:
        symbol = getattr(contract, 'symbol', str(contract))
        barSize = timeframe
        durationStr = "1 W"
        print(contract)
        print(durationStr)
        print(barSize)
        
        
        try:
            bars = self.ib.reqHistoricalData(contract, endDateTime='', durationStr=durationStr,
                                             barSizeSetting=barSize, whatToShow='TRADES', useRTH=True)
            self.ib.sleep(1)
        except Exception as e:
            logger.exception("Error fetching historical bars: %s", e)
            return None
        if not bars:
            return None
        df = util.df(bars)
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
            df = df.set_index('date')
        df = df.rename(columns={'open': 'open', 'high': 'high', 'low': 'low', 'close': 'close', 'volume': 'volume'})
        return df

    def get_ticker(self, contract):
        return self.ib.ticker(contract)

    def get_vix(self):
        pass

    def clean_cache(self):
        pass
