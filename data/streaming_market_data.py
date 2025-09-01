import asyncio
import logging
from collections import defaultdict
from typing import Dict, Callable, Optional, List, Any, Tuple
import pandas as pd
from datetime import datetime, timezone
from ib_async import IB, Contract, util


TF_MINUTES = {
    '1 min': 1,
    '5 mins': 5,
    '30 mins': 30, 
    '1 day': 1,
    '1 week': 1
}

class StreamingMarketData:
    def __init__(self, ib: IB, logger, bar_seconds: int = 60, buffer_minutes: int = 240):
        self.ib = ib
        self.bar_seconds = int(bar_seconds)
        self.buffer_minutes = int(buffer_minutes)
        self._subscribed: Dict[str, Dict[str, Any]] = {}
        self._data_1m: Dict[str, pd.DataFrame] = {}
        self._data_htf: Dict[str, Dict[str, pd.DataFrame]] = defaultdict(dict)
        self._callbacks: Dict[Tuple, List[Callable]] = defaultdict(list)
        self._lock = asyncio.Lock()
        # Subscribe to pendingTickersEvent instead of nonexistent realTimeBarEvent
        self.ib.pendingTickersEvent += self._ib_realtime_bar_handler
        
        self.logger = logger
        self.logger.info("🆕📡 StreamingMarketData initialized with bar_seconds=%d, buffer_minutes=%d", bar_seconds, buffer_minutes)

    async def seed_history(self, contract: Contract, n_minutes: int = 2400) -> Optional[pd.DataFrame]:
        symbol = contract.symbol
        total_seconds = max(60, n_minutes * 60)
        durationStr = f"{total_seconds} S"
        try:
            self.logger.info("📜⏳ Seeding %d min historical data for %s ...", n_minutes, symbol)
            bars = self.ib.reqHistoricalData(
                contract,
                endDateTime='',
                durationStr=durationStr,
                barSizeSetting='1 min',
                whatToShow='TRADES',
                useRTH=True
            )
        except Exception:
            self.logger.exception("📜❌ Failed to seed historical bars for %s", symbol)
            return None
        if not bars:
            self.logger.warning("📜⚠️ No historical bars returned for %s", symbol)
            return None
        df = util.df(bars)
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
            df = df.set_index('date')
        required_cols = ['open', 'high', 'low', 'close', 'volume']
        for col in required_cols:
            if col not in df.columns:
                self.logger.warning("📜⚠️ Seeded historical data for %s missing column %s; skipping", symbol, col)
                return None
        df = df[required_cols]
        df = df.iloc[-n_minutes:].copy()
        self.logger.info("📜✅ Seeded history for %s (%d bars)", symbol, len(df))
        return df


    async def seed_history_new(self, contract: Contract, n_minutes: int = 240) -> Optional[pd.DataFrame]:
        symbol = contract.symbol
        total_minutes = n_minutes
        bar_size = '1 min'
        max_duration_per_request = 60  # IB recommends 60 minutes or less per chunk for 1-min bars
        end_dt = datetime.now()  # Use naive datetime; set appropriate tz if needed
        all_bars = []
        
        self.logger.info(f"📜⏳ Seeding {n_minutes} min historical data for {symbol} with pagination ...")
    
        while total_minutes > 0:
            chunk = min(total_minutes, max_duration_per_request)
            durationStr = f"{chunk} S" if chunk < 60 else f"{chunk} M"
            endDateTime = end_dt.strftime('%Y%m%d %H:%M:%S')
            try:
                self.logger.info(f"🧩 Requesting {chunk} min from {endDateTime} for {symbol}")
                bars = self.ib.reqHistoricalData(
                    contract,
                    endDateTime=endDateTime,
                    durationStr=durationStr,
                    barSizeSetting=bar_size,
                    whatToShow='TRADES',
                    useRTH=True
                )
            except Exception:
                self.logger.exception(f"📜❌ Failed to seed {chunk} bars for {symbol}")
                return None
            if not bars:
                self.logger.warning(f"📜⚠️ No bars returned for {symbol} at end {endDateTime}")
                break
    
            df = util.df(bars)
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'])
                df = df.set_index('date')
            all_bars.append(df)
            fetched = len(df)
            self.logger.info(f"🟦 Got {fetched} bars for {symbol} from {df.index[-1] if not df.empty else '?'}")
            if fetched < chunk:
                self.logger.warning(f"🔚 Reached earliest data available for {symbol}.")
                break
    
            # Move end_dt backward by the number of fetched minutes
            end_dt = df.index.min() - pd.Timedelta(minutes=1)
            total_minutes -= fetched
    
        if not all_bars:
            self.logger.warning(f"📜⚠️ No historical bars returned for {symbol}")
            return None
    
        df_full = pd.concat(all_bars)
        df_full = df_full.sort_index()
        required_cols = ['open', 'high', 'low', 'close', 'volume']
        for col in required_cols:
            if col not in df_full.columns:
                self.logger.warning(f"📜⚠️ Missing column {col} for {symbol}; aborting")
                return None
    
        df_full = df_full[required_cols]
        df_full = df_full.iloc[-n_minutes:].copy()  # Trim to desired length
        self.logger.info(f"📜✅ Seeded paginated history for {symbol} ({len(df_full)} bars)")
        return df_full

    async def subscribe_old(self, contract: Contract, seed_minutes: int = 240) -> bool:
        symbol = contract.symbol
        self.logger.info("📡🔄 Subscribing to 1-min bars for %s ...", symbol)
        if symbol in self._subscribed:
            self.logger.debug("📡♻️ Already subscribed to %s", symbol)
            return True
        df = await self.seed_history(contract, n_minutes=seed_minutes)
        if df is None or df.empty:
            self.logger.warning("📜⚠️ No historical data to seed for %s; continuing but buffer will be empty", symbol)
            df = pd.DataFrame(columns=['open', 'high', 'low', 'close', 'volume'])
        if len(df) > self.buffer_minutes:
            self.logger.debug("📉✂️ Truncating %s history to the last %d minutes", symbol, self.buffer_minutes)
            df = df.iloc[-self.buffer_minutes:].copy()
        async with self._lock:
            self._data_1m[symbol] = df
            self._data_htf[symbol]['5 mins'] = self._aggregate(df, 5)
            self._data_htf[symbol]['30 mins'] = self._aggregate(df, 30)
        try:
            reqId = self.ib.reqRealTimeBars(contract, barSize=60, whatToShow='TRADES', useRTH=True, realTimeBarsOptions=[])
            self.logger.info("📡✅ Sent realTimeBars req for %s (reqId=%s)", symbol, reqId)
        except Exception as e:
            self.logger.exception("📡❌ Failed to request realTimeBars for %s", symbol)
            return False
        self._subscribed[symbol] = {'contract': contract, 'reqId': reqId}
        self.logger.info("📡🟢 Subscribed to 1-min bars for %s (reqId=%s)", symbol, reqId)
        return True
    
    async def subscribe(self, contract: Contract, seed_minutes: int = 240) -> bool:
        symbol = contract.symbol
        self.logger.info("📡🔄 Subscribing to 1-min bars for %s ...", symbol)
        if symbol in self._subscribed:
            self.logger.debug("📡♻️ Already subscribed to %s", symbol)
            return True
    
        # Seed historical data as before
        df = await self.seed_history(contract, n_minutes=seed_minutes)
        if df is None or df.empty:
            self.logger.warning("📜⚠️ No historical data to seed for %s; continuing but buffer will be empty", symbol)
            df = pd.DataFrame(columns=['open', 'high', 'low', 'close', 'volume'])
        if len(df) > self.buffer_minutes:
            self.logger.debug("📉✂️ Truncating %s history to the last %d minutes", symbol, self.buffer_minutes)
            df = df.iloc[-self.buffer_minutes:].copy()
        async with self._lock:
            self._data_1m[symbol] = df
            self._data_htf[symbol]['5 mins'] = self._aggregate(df, 5)
            self._data_htf[symbol]['30 mins'] = self._aggregate(df, 30)
    
        try:
            # REQUEST LIVE MARKET DATA INSTEAD OF reqRealTimeBars
            ticker = self.ib.reqMktData(contract, "", False, False)
            await asyncio.sleep(2)  # Wait for initialization
    
            # BARS AGGREGATION (eventkit -- 1 min bars)
            import eventkit as ev
            bar_stream = ticker.updateEvent.trades().timebars(ev.Timer(60))
    
            # Start background task for processing live bars
            if not hasattr(self, "_bar_tasks"):
                self._bar_tasks = {}
            async def process_bars():
                async for bar in bar_stream.aiter():
                    self.logger.info("New 1-min bar: %s for %s", bar, symbol)
                    # (Optionally store, publish, or process bar here)
            self._bar_tasks[symbol] = asyncio.create_task(process_bars())
    
            self.logger.info("📡✅ Subscribed to 1-min bars for %s (custom, via timebars)", symbol)
            self._subscribed[symbol] = {"contract": contract, "ticker": ticker}
            return True
        except Exception as e:
            self.logger.exception("📡❌ Failed to subscribe to live bars for %s", symbol)
            return False


    def _aggregate_old(self, df_1m: pd.DataFrame, minutes: int) -> pd.DataFrame:
        self.logger.info(f'minutes in aggregate===> {minutes}')
        if df_1m is None or df_1m.empty:
            self.logger.debug("📊⚠️ No data to aggregate for %d-min bars", minutes)
            return pd.DataFrame(columns=['open', 'high', 'low', 'close', 'volume'])
        offset = f"{minutes}T"
        self.logger.debug("📊🔢 Aggregating %d-min bars", minutes)
        df = df_1m.resample(offset).agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        }).dropna()
        self.logger.debug("📊✅ Aggregated %d-min bars (%d rows)", minutes, len(df))
        return df
    
    def _aggregate(self, df_1m: pd.DataFrame, minutes: int) -> pd.DataFrame:
        self.logger.info(f'minutes in aggregate===> {minutes}')
        if df_1m is None or df_1m.empty:
            self.logger.debug("📊⚠️ No data to aggregate for %d-min bars", minutes)
            return pd.DataFrame(columns=['open', 'high', 'low', 'close', 'volume'])
    
        # Robust timezone normalization
        idx = df_1m.index
        if isinstance(idx, pd.DatetimeIndex):
            if idx.tz is not None:
                # Convert to UTC and remove tz info for resampling
                df_1m.index = idx.tz_convert('UTC').tz_localize(None)
            else:
                df_1m.index = pd.to_datetime(idx)
        else:
            # Try convert index as naive but force UTC then remove
            df_1m.index = pd.to_datetime(idx, utc=True).tz_localize(None)
    
        assert isinstance(df_1m.index, pd.DatetimeIndex), f"Index is {type(df_1m.index)}"
        offset = f"{minutes}min"
        self.logger.debug("📊🔢 Aggregating %d-min bars", minutes)
        df = df_1m.resample(offset).agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        }).dropna()
        self.logger.debug("📊✅ Aggregated %d-min bars (%d rows)", minutes, len(df))
        return df



    async def _ib_realtime_bar_handler(self, tickers):
        """Handle pendingTickersEvent containing a set of Ticker objects."""
        async with self._lock:
            for ticker in tickers:
                symbol = ticker.contract.symbol if ticker.contract else None
                if not symbol or symbol not in self._subscribed:
                    self.logger.debug("📡⚠️ Ticker received for unknown or unsubscribed symbol: %s", symbol)
                    continue
                time_dt = ticker.time
                if time_dt is None:
                    time_dt = datetime.now(timezone.utc)
                else:
                    if isinstance(time_dt, int):
                        time_dt = datetime.fromtimestamp(time_dt, timezone.utc)
                    else:
                        time_dt = pd.to_datetime(time_dt)
                        if time_dt.tzinfo is None:
                            time_dt = time_dt.tz_localize(timezone.utc)
                time_dt = time_dt.replace(second=0, microsecond=0)
                row = {
                    'open': ticker.open,
                    'high': ticker.high,
                    'low': ticker.low,
                    'close': ticker.close,
                    'volume': ticker.volume
                }
                df1 = self._data_1m.get(symbol)
                if df1 is None:
                    self.logger.debug("📄 Initialized new empty DataFrame for %s realtime bars", symbol)
                    df1 = pd.DataFrame(columns=['open', 'high', 'low', 'close', 'volume'])
                df1 = pd.concat([df1, pd.DataFrame([row], index=[time_dt])])
                df1 = df1[~df1.index.duplicated(keep='last')]
                if len(df1) > self.buffer_minutes:
                    self.logger.debug("📈✂️ Purging oldest bars from buffer for %s", symbol)
                    df1 = df1.iloc[-self.buffer_minutes:].copy()
                self._data_1m[symbol] = df1
                self.logger.info("📥⏲️ Appended new 1-min bar for %s @ %s", symbol, time_dt)
                await self._notify_callbacks(symbol, '1 min', df1)
                for tf in ('5 mins', '30 mins'):
                    prev_agg = self._data_htf.get(symbol, {}).get(tf, pd.DataFrame())
                    new_agg = self._aggregate(df1, TF_MINUTES[tf])
                    self._data_htf[symbol][tf] = new_agg
                    if not prev_agg.empty and not new_agg.empty and prev_agg.index[-1] != new_agg.index[-1]:
                        self.logger.info("📈🆕 New %s bar for %s @ %s", tf, symbol, new_agg.index[-1])
                        await self._notify_callbacks(symbol, tf, new_agg)

    async def _notify_callbacks(self, symbol: str, timeframe: str, df: pd.DataFrame):
        key = (symbol, timeframe)
        wildcard_key = (symbol, 'any')
        callbacks = []
        for k in (key, wildcard_key):
            callbacks.extend(self._callbacks.get(k, []))
        if not callbacks:
            self.logger.debug("🔔 No callbacks registered for %s %s", symbol, timeframe)
            return
        tasks = []
        for cb in callbacks:
            try:
                self.logger.debug("🔔 Scheduling callback for %s %s", symbol, timeframe)
                tasks.append(asyncio.create_task(cb(symbol, timeframe, df)))
            except Exception:
                self.logger.exception("🔔❌ Callback scheduling failed for %s %s", symbol, timeframe)
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
            self.logger.debug("🔔✅ Notified callbacks for %s %s", symbol, timeframe)

    def on_bar(self, symbol: str, timeframe: str, callback: Callable[[str, str, pd.DataFrame], Any]):
        key = (symbol, timeframe)
        self._callbacks[key].append(callback)
        self.logger.info("🔔 Registered callback for %s %s", symbol, timeframe)

    def get_latest(self, symbol: str, timeframe: str) -> Optional[pd.DataFrame]:
        if timeframe == '1 min':
            return self._data_1m.get(symbol)
        return self._data_htf.get(symbol, {}).get(timeframe)

    async def unsubscribe(self, symbol: str):
        sub = self._subscribed.pop(symbol, None)
        if not sub:
            self.logger.info("📡⚠️ No active subscription found for %s (already unsubscribed?)", symbol)
            return
        reqId = sub.get('reqId')
        if reqId is None:
            self.logger.warning("📡❓ Subscription for %s has no reqId; skipping unsubscribe", symbol)
            return
        try:
            self.ib.cancelRealTimeBars(reqId)
            self.logger.info("📡🛑 Cancelled realTimeBars for %s (reqId=%s)", symbol, reqId)
        except Exception:
            try:
                contract = sub.get('contract')
                if contract:
                    self.ib.cancelRealTimeBars(contract)
                    self.logger.info("📡🛑 Cancelled realTimeBars by contract for %s", symbol)
            except Exception:
                self.logger.exception("📡❌ Failed to cancel realTimeBars for %s (reqId=%s)", symbol, reqId)
        async with self._lock:
            self._data_1m.pop(symbol, None)
            self._data_htf.pop(symbol, None)
        self.logger.info("📡❎ Unsubscribed from %s", symbol)

    async def close(self):
        self.logger.info("🔒 Closing all streaming market data subscriptions ...")
        for sym in list(self._subscribed.keys()):
            await self.unsubscribe(sym)
        try:
            self.ib.pendingTickersEvent -= self._ib_realtime_bar_handler
            self.logger.info("🔕 Unregistered real-time bar handler")
        except Exception:
            self.logger.warning("🔕❌ Failed to unregister real-time bar handler (may not have been set)")
