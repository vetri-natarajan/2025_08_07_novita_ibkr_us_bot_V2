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

class HistoricalFetcher:
    def __init__(self, ib, logger, max_bars_per_request=100):
        self.ib = ib
        self.logger = logger
        self.max_bars_per_request = max_bars_per_request
        self.executor = ThreadPoolExecutor(max_workers=5)  # Proper executor for blocking calls

    async def fetch_bars(self, contract: Contract, end_dt: datetime, durationStr: str, barSize: str) -> Optional[pd.DataFrame]:
        end_str = end_dt.strftime("%Y%m%d %H:%M:%S")
        try:
            loop = asyncio.get_event_loop()
            bars = await loop.run_in_executor(
                self.executor,
                lambda: self.ib.reqHistoricalData(
                    contract,
                    end_str,
                    durationStr,
                    barSize,
                    whatToShow="TRADES",
                    useRTH=True,
                    formatDate=1
                )
            )
        except Exception as e:
            self.logger.error(f"❌ Error fetching bars for {contract.symbol} at {end_str}: {e}")
            return None

        if not bars:
            self.logger.warning(f"⚠️ No bars received for {contract.symbol} at {end_str}")
            return None

        df = util.df(bars)
        if 'date' not in df.columns:
            self.logger.error(f"❌ Bars missing 'date' for {contract.symbol}")
            return None

        df['date'] = pd.to_datetime(df['date'])
        df.set_index('date', inplace=True)

        req_cols = ['open', 'high', 'low', 'close', 'volume']
        if not all(col in df.columns for col in req_cols):
            self.logger.error(f"❌ Missing required cols in bars for {contract.symbol}")
            return None

        return df[req_cols]

    async def seed_historical_paginated(self, contract: Contract, timeframe: str, max_tf: str, max_look_back: int):
        bars_remaining = max_look_back
        tf_map = MAX_TF_TO_LTF.get(max_tf, {})
        bars_per_request = tf_map.get(timeframe, self.max_bars_per_request)

        end_dt = datetime.now(timezone.utc)
        all_dfs = []

        self.logger.info(f"⌛️ Starting paginated fetch for {contract.symbol} [{timeframe}] aiming for {max_look_back} bars")

        while bars_remaining > 0:
            batch_size = min(bars_remaining, bars_per_request)
            durationStr = f"{int(batch_size)} M" if batch_size >= 1 else f"{int(batch_size*60)} S"
            self.logger.info(f"📦 Requesting {batch_size} bars ending {end_dt} for {contract.symbol} [{timeframe}]")

            df_chunk = await self.fetch_bars(contract, end_dt, durationStr, timeframe)
            if df_chunk is None or df_chunk.empty:
                self.logger.warning(f"⚠️ No data for chunk ending {end_dt} for {contract.symbol} [{timeframe}]")
                break

            bars_fetched = len(df_chunk)
            bars_remaining -= bars_fetched
            all_dfs.append(df_chunk)

            self.logger.info(f"✅ Received {bars_fetched} bars, {bars_remaining} left for {contract.symbol} [{timeframe}]")
            end_dt = df_chunk.index.min() - timedelta(seconds=1)  # Avoid overlap

        if not all_dfs:
            self.logger.warning(f"⚠️ No data fetched for {contract.symbol} [{timeframe}] after pagination")
            return None

        full_df = pd.concat(all_dfs).sort_index()
        self.logger.info(f"🎉 Completed fetch for {contract.symbol} [{timeframe}]: total {len(full_df)} bars")
        return full_df

class StreamingData(HistoricalFetcher):
    def __init__(self, ib: IB, logger: logging.Logger, buffer_limit: int = 1000):
        super().__init__(ib, logger)
        self.buffer_limit = buffer_limit
        self.executor = ThreadPoolExecutor(max_workers=5)
        self._lock = asyncio.Lock()
        self._subscriptions: Dict[str, Dict[str, dict]] = defaultdict(dict)
        self._data_cache: Dict[str, Dict[str, pd.DataFrame]] = defaultdict(dict)
        self._callbacks: Dict[Tuple[str, str], List[Callable]] = defaultdict(list)
        self.ib.pendingTickersEvent += self._ib_pending_tickers_handler
        logger.info(f"🚀 StreamingData initialized with buffer limit {buffer_limit} bars")

    async def seed_historical(self, contract, timeframe, max_tf, max_look_back):
        self.logger.info(f"⌛️ Starting seed_historical for {contract.symbol} [{timeframe}]")
        df = await self.seed_historical_paginated(contract, timeframe, max_tf, max_look_back)
        if df is None or df.empty:
            self.logger.warning(f"⚠️ No data fetched in seed_historical for {contract.symbol} [{timeframe}]")
        else:
            self.logger.info(f"✅ seed_historical fetched {len(df)} bars for {contract.symbol} [{timeframe}]")
        return df

    async def subscribe(self, contract: Contract, timeframe: str, max_tf: str, max_look_back: int) -> bool:
        symbol = contract.symbol
        async with self._lock:
            if timeframe in self._subscriptions[symbol]:
                self.logger.info(f"♻️ Already subscribed: {symbol} [{timeframe}]")
                return True

            df = await self.seed_historical(contract, timeframe, max_tf, max_look_back)
            if df is None:
                df = pd.DataFrame(columns=['open','high','low','close','volume'])

            self._data_cache[symbol][timeframe] = df

            try:
                bar_size_sec = TF_TO_MINUTES.get(timeframe, 1)*60
                req_id = self.ib.reqRealTimeBars(contract, bar_size_sec, "TRADES", True, [])
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
                if len(df) > self.buffer_limit:
                    df = df.tail(self.buffer_limit)
                self._data_cache[symbol][timeframe] = df

                self.logger.info(f"🆕 New {timeframe} bar for {symbol} at {bar_time}")
                await self._fire_callbacks(symbol, timeframe, df)

    async def _fire_callbacks(self, symbol: str, timeframe: str, df: pd.DataFrame):
        key = (symbol, timeframe)
        callbacks = self._callbacks.get(key, [])
        if not callbacks:
            self.logger.debug(f"🔕 No callbacks for {symbol} [{timeframe}]")
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
