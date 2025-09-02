import asyncio
import logging
from collections import defaultdict
from typing import Dict, Callable, Optional, List, Tuple
import pandas as pd
from datetime import datetime, timezone, timedelta
from ib_async import IB, Contract, util
from concurrent.futures import ThreadPoolExecutor

TF_TO_MINUTES = {
    '1 min': 1,
    '5 mins': 5,
    '30 mins': 30,
    '1 day': 60*24,
    '1 week': 60*24*7
}

TF_TO_IB_SIZE = {
    '1 min': '1 min',
    '5 mins': '5 mins',
    '30 mins': '30 mins',
    '1 day': '1 day',
    '1 week': '1 week'
}

MAX_TF_TO_LTF = {
    '1 week': {'1 week': 1, '1 day': 5, '5 mins': 78*5},
    '30 mins': {'30 mins': 1, '5 mins': 6, '1 min': 30}
}

class StreamingData:
    def __init__(self, ib: IB, logger: logging.Logger, buffer_limit: int = 1000):
        self.ib = ib
        self.logger = logger
        self.buffer_limit = buffer_limit
        self.executor = ThreadPoolExecutor(max_workers=5)  # limit number of threads
        self._lock = asyncio.Lock()
        self._subscriptions: Dict[str, Dict[str, dict]] = defaultdict(dict)  # symbol -> timeframe -> subscription info
        self._data_cache: Dict[str, Dict[str, pd.DataFrame]] = defaultdict(dict)  # symbol -> timeframe -> df
        self._callbacks: Dict[Tuple[str,str], List[Callable]] = defaultdict(list)
        self.ib.pendingTickersEvent += self._ib_pending_tickers_handler
        self.logger.info(f"🚀 StreamingData initialized with buffer limit {buffer_limit} bars.")

    async def seed_historical(self, contract: Contract, timeframe: str, max_tf: str, max_look_back: int) -> Optional[pd.DataFrame]:
        duration_value  = int(round(max_look_back*1.5))
        self.logger.info(f"⌛️ Calculating duration string for seeding {contract.symbol} {timeframe} bars (max_tf: {max_tf})")

        if max_tf.lower() == "1 week":           
            division_factor = MAX_TF_TO_LTF.get(max_tf).get(timeframe)
            duration_str_value = int(duration_value/division_factor)
            if duration_str_value == 0 and timeframe == '1 day':
                duration_str_value = 5
            duration_str = f"{duration_str_value} W"  # 📅 Weekly timeframe
            self.logger.info(f"📅 Duration string set to {duration_str} (weekly)")
        
        elif max_tf.lower() == "30 mins":
            division_factor = MAX_TF_TO_LTF.get(max_tf).get(timeframe)
            duration_str_value = int(duration_value/division_factor)
            if duration_str_value == 0 and timeframe == '1 min':
                duration_str_value = 2
            duration_str = f"{duration_str_value} D"  # ⏱️ Intraday timeframe
            
            self.logger.info(f"⏱️ Duration string set to {duration_str} (intraday)")
        
        else: 
            err_msg = "⚠️ Invalid max timeframe. Should be '1 week' or '30 mins'."
            self.logger.error(err_msg)
            raise ValueError(err_msg)

        try:
            bars_raw = await asyncio.get_event_loop().run_in_executor(
                self.executor,
                lambda: self.ib.reqHistoricalData(
                    contract,
                    endDateTime='',
                    durationStr=duration_str,
                    barSizeSetting=timeframe,
                    whatToShow='TRADES',
                    useRTH=True
                )
            )
        except Exception as e:
            self.logger.error(f"❌ Failed to fetch historical {timeframe} data for {contract.symbol}: {e}")
            return None
        
        if not bars_raw:
            self.logger.warning(f"⚠️ No historical bars returned for {contract.symbol} [{timeframe}]")
            return None
        
        df = util.df(bars_raw)
        if 'date' not in df.columns:
            self.logger.error(f"❌ Historical data for {contract.symbol} missing 'date' column.")
            return None
        
        df['date'] = pd.to_datetime(df['date'])
        df.set_index('date', inplace=True)
        required_cols = ['open','high','low','close','volume']
        if not all(col in df.columns for col in required_cols):
            self.logger.error(f"❌ Missing required columns in historical data for {contract.symbol} [{timeframe}]")
            return None
        '''
        df = df[required_cols].copy()
        if len(df) > self.buffer_limit:
            self.logger.info(f"✂️ Truncating historical data to last {self.buffer_limit} bars for {contract.symbol} [{timeframe}]")
            df = df.tail(self.buffer_limit)
        '''
        self.logger.info(f'df_timeframe ===> {timeframe}')
        self.logger.info(df.tail)
        self.logger.info(f"✅ Seeded {len(df)} bars for {contract.symbol} [{timeframe}]")
        return df

    async def subscribe(self, contract: Contract, timeframe: str, max_tf: str, max_look_back: int) -> bool:
        symbol = contract.symbol
        async with self._lock:
            if timeframe in self._subscriptions[symbol]:
                self.logger.info(f"♻️ Already subscribed: {symbol} [{timeframe}]")
                return True
            
            self.logger.info(f"🌱 Starting subscription by seeding history for {symbol} [{timeframe}]")
            df = await self.seed_historical(contract, timeframe, max_tf, max_look_back)
            if df is None:
                df = pd.DataFrame(columns=['open','high','low','close','volume'])
                self.logger.warning(f"⚠️ Starting with empty data for {symbol} [{timeframe}] after failed seed")
            
            self._data_cache[symbol][timeframe] = df
            
            try:
                bar_size_sec = TF_TO_MINUTES.get(timeframe, 1)*60
                self.logger.info(f"Contract in reqrealtimebars {contract} ")
                req_id = self.ib.reqRealTimeBars(contract, 5, "TRADES", True, [])
                self._subscriptions[symbol][timeframe] = {'contract': contract, 'req_id': req_id}
                self.logger.info(f"📡 Subscribed to {symbol} [{timeframe}] with reqId {req_id}")
                return True
            except Exception as e:
                self.logger.error(f"❌ Failed to subscribe to {symbol} [{timeframe}]: {e}")
                return False

    async def _ib_pending_tickers_handler(self, tickers):
        async with self._lock:
            for ticker in tickers:
                contract = ticker.contract
                if not contract:
                    continue
                symbol = contract.symbol
                timeframe = None
                for tf, info in self._subscriptions.get(symbol, {}).items():
                    if info.get('req_id') == ticker.reqId:
                        timeframe = tf
                        break
                if not timeframe:
                    continue
                
                bar_time = ticker.time
                if isinstance(bar_time, int):
                    bar_time = datetime.fromtimestamp(bar_time, timezone.utc)
                elif isinstance(bar_time, str):
                    bar_time = pd.to_datetime(bar_time)
                if bar_time.tzinfo is None:
                    bar_time = bar_time.tz_localize(timezone.utc)
                bar_time = bar_time.replace(second=0, microsecond=0)
                
                row = {
                    'open': ticker.open,
                    'high': ticker.high,
                    'low': ticker.low,
                    'close': ticker.close,
                    'volume': ticker.volume
                }
                
                df = self._data_cache.get(symbol, {}).get(timeframe)
                if df is None:
                    df = pd.DataFrame(columns=['open','high','low','close','volume'])
                
                df_new = pd.DataFrame([row], index=[bar_time])
                df = pd.concat([df, df_new])
                df = df[~df.index.duplicated(keep='last')]
                '''
                if len(df) > self.buffer_limit:
                    self.logger.info(f"✂️ Truncating live data to last {self.buffer_limit} bars for {symbol} [{timeframe}]")
                    df = df.tail(self.buffer_limit)
                '''
                self._data_cache[symbol][timeframe] = df
                
                self.logger.info(f"🆕 New {timeframe} bar for {symbol} at {bar_time}")
                await self._fire_callbacks(symbol, timeframe, df)

    async def _fire_callbacks(self, symbol: str, timeframe: str, df: pd.DataFrame):
        key = (symbol, timeframe)
        callbacks = self._callbacks.get(key, [])
        if not callbacks:
            self.logger.debug(f"🔕 No callbacks registered for {symbol} [{timeframe}]")
            return
        self.logger.debug(f"🔔 Triggering {len(callbacks)} callbacks for {symbol} [{timeframe}]")
        tasks = []
        for cb in callbacks:
            try:
                tasks.append(asyncio.create_task(cb(symbol, timeframe, df)))
            except Exception as e:
                self.logger.error(f"❌ Failed to schedule callback for {symbol} [{timeframe}]: {e}")
        if tasks:
            await asyncio.gather(*tasks)

    def on_bar(self, symbol: str, timeframe: str, callback: Callable):
        self._callbacks[(symbol, timeframe)].append(callback)
        self.logger.info(f"📝 Registered callback for {symbol} [{timeframe}]")

    def get_latest(self, symbol: str, timeframe: str) -> Optional[pd.DataFrame]:
        return self._data_cache.get(symbol, {}).get(timeframe)

    async def unsubscribe(self, symbol: str):
        async with self._lock:
            subs = self._subscriptions.pop(symbol, {})
            for tf, info in subs.items():
                req_id = info.get('req_id')
                if req_id is not None:
                    try:
                        self.ib.cancelRealTimeBars(req_id)
                        self.logger.info(f"🛑 Unsubscribed realtime bars for {symbol} [{tf}]")
                    except Exception as e:
                        self.logger.error(f"❌ Error unsubscribing {symbol} [{tf}]: {e}")
            self._data_cache.pop(symbol, None)
            self.logger.info(f"🗑️ Cleared data cache and subscriptions for {symbol}")

    async def close(self):
        async with self._lock:
            symbols = list(self._subscriptions.keys())
            for sym in symbols:
                await self.unsubscribe(sym)
            try:
                self.ib.pendingTickersEvent -= self._ib_pending_tickers_handler
                self.logger.info(f"🛑 Unregistered IB event handler for realtime bars")
            except Exception:
                self.logger.warning(f"⚠️ IB event handler was not registered or already removed")
