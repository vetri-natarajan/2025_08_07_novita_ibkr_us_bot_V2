import asyncio
import functools
import logging
from collections import defaultdict, deque
from typing import Dict, Callable, Optional, List, Tuple, Any
import pandas as pd
from datetime import datetime, timedelta
from dataclasses import dataclass
from ib_async import IB, Contract, util
from concurrent.futures import ThreadPoolExecutor
import pytz

# --------------------------
# Explicit aggregation graph
# --------------------------
# target_timeframe -> (parent_timeframe, count_of_parent_bars)
PARENT_TF = {
    '1 min':   ('5 secs', 12),  # 12 × 5s = 60s
    '2 mins':  ('1 min', 2),    # 2 × 1m = 2m  
    '3 mins':  ('1 min', 3),    # 3 × 1m = 3m      
    '5 mins':  ('1 min', 5),    # 5 × 1m = 5m
    '30 mins': ('5 mins', 6),   # 6 × 5m = 30m
    '1 day':   ('5 mins', 78),  # 78 × 5m = 390 minutes (US equities RTH)
    '1 week':  ('1 day', 5),    # 5 × 1d = 1w (trading days)
}

# Kept for historical duration heuristics only if needed; not used for aggregation math
TF_TO_MINUTES = {
    '1 min': 1,
    '2 mins': 2,
    '3 mins': 3,
    '5 mins': 5,
    '30 mins': 30,
    '1 day': 24 * 60,   # not used for aggregation math (RTH vs full day differs)
    '1 week': 5 * 24 * 60,  # naive placeholder
}

MAX_TF_TO_LTF = {
    '1 week': {'1 week': 1, '1 day': 5, '5 mins': 78 * 5},
    '30 mins': {'30 mins': 1, '5 mins': 6, '1 min': 30},
    '3 mins': {'3 mins': 1, '2 mins': 1.5, '1 min': 3},
}

@dataclass
class AggregatedBar:
    time: datetime
    open_: float
    high: float
    low: float
    close: float
    volume: float

def floor_time_to_tf(dt: datetime, target_tf: str, tz) -> datetime:
    """
    Floor timestamp to the start of the target timeframe bucket.
    For production, replace daily/weekly with session-aware logic via an exchange calendar.
    """
    if dt.tzinfo is None:
        dt = pytz.UTC.localize(dt)
    dt = dt.astimezone(tz)

    if target_tf == '5 secs':
        return dt.replace(second=(dt.second // 5) * 5, microsecond=0)
    if target_tf == '1 min':
        return dt.replace(second=0, microsecond=0)
    if target_tf == '2 mins':
        m = (dt.minute // 2) * 2
        return dt.replace(minute=m, second=0, microsecond=0)
    if target_tf == '3 mins':
        m = (dt.minute // 3) * 3
        return dt.replace(minute=m, second=0, microsecond=0)
    if target_tf == '5 mins':
        m = (dt.minute // 5) * 5
        return dt.replace(minute=m, second=0, microsecond=0)
    if target_tf == '30 mins':
        m = (dt.minute // 30) * 30
        return dt.replace(minute=m, second=0, microsecond=0)
    if target_tf == '1 day':
        # Simplified: calendar day in tz
        return dt.replace(hour=0, minute=0, second=0, microsecond=0)
    if target_tf == '1 week':
        # Simplified: week starts Monday in tz
        start = dt - timedelta(days=dt.weekday())
        return start.replace(hour=0, minute=0, second=0, microsecond=0)
    # Fallback: zero microseconds
    return dt.replace(microsecond=0)

class BarAggregator:
    """
    Timeframe-aware aggregator that consumes parent timeframe bars and emits target timeframe bars.
    """
    def __init__(self, target_timeframe: str, parent_timeframe: str, count: int, logger, tz):
        self.target_tf = target_timeframe
        self.parent_tf = parent_timeframe
        self.count = count
        self.logger = logger
        self.tz = tz
        self.bars = deque()  # parent bars buffer

    def add_bar(self, bar: AggregatedBar) -> Optional[AggregatedBar]:
        """
        Add a parent bar. When 'count' parent bars collected, emit a completed target bar.
        """
        if bar is None:
            self.logger.error("Attempted to add None to aggregator buffer!")
            return None
        self.bars.append(bar)
        if len(self.bars) == self.count:
            agg_bar = self.aggregate_bars(list(self.bars))
            self.bars.clear()
            self.logger.debug(f"Aggregated {self.count} [{self.parent_tf}] bars into [{self.target_tf}] at {agg_bar.time}")
            return agg_bar
        return None

    def partial_aggregate_bars(self) -> Optional[AggregatedBar]:
        """
        Produce a partial aggregate for UI/preview. Not a finalized target bar.
        """
        if not self.bars:
            return None
        bars = list(self.bars)
        return self._aggregate_core(bars)

    def aggregate_bars(self, bars: List[AggregatedBar]) -> AggregatedBar:
        """
        Aggregate exactly 'count' parent bars into a completed target bar.
        """
        return self._aggregate_core(bars)

    def _aggregate_core(self, bars: List[AggregatedBar]) -> AggregatedBar:
        open_ = bars[0].open_
        high = max(b.high for b in bars)
        low = min(b.low for b in bars)
        close = bars[-1].close
        volume = sum(b.volume for b in bars)
        last_time = bars[-1].time
        time = floor_time_to_tf(last_time, self.target_tf, self.tz)
        return AggregatedBar(time, open_, high, low, close, volume)

class StreamingData:
    def __init__(self, ib: IB, logger: logging.Logger, trading_time_zone: str, ib_connector, buffer_limit: int = 1000):
        self.ib = ib
        self.logger = logger
        self.time_zone = trading_time_zone
        self.ib_connector = ib_connector
        self.buffer_limit = buffer_limit

        self.executor = ThreadPoolExecutor(max_workers=5)
        self._lock = asyncio.Lock()

        # symbol -> timeframe -> subscription info
        self._subscriptions: Dict[str, Dict[str, dict]] = defaultdict(dict)

        # symbol -> timeframe -> df
        self._data_cache: Dict[str, Dict[str, pd.DataFrame]] = defaultdict(dict)

        # (symbol, timeframe) -> [callbacks]
        self._callbacks: Dict[Tuple[str, str], List[Callable]] = defaultdict(list)

        # (symbol, timeframe) -> BarAggregator
        self._aggregators: Dict[Tuple[str, str], BarAggregator] = {}

        # symbol -> ticker or bars handle
        self.tickers: Dict[str, Any] = {}

        # which symbols have live tick subscriptions
        self.live_tick_subscriptions = set()

        self.logger.info(f"🚀 StreamingData initialized with buffer limit {buffer_limit} bars.")

    def get_latest_partial_bar(self, symbol: str, timeframe: str) -> Optional[AggregatedBar]:
        df = self._data_cache.get(symbol, {}).get(timeframe)
        if df is not None and not df.empty:
            last_row = df.iloc[-1]
            try:
                time = df.index[-1]
                return AggregatedBar(
                    time=time,
                    open_=float(last_row['open']),
                    high=float(last_row['high']),
                    low=float(last_row['low']),
                    close=float(last_row['close']),
                    volume=float(last_row['volume'])
                )
            except Exception as e:
                self.logger.error(f"Error reconstructing latest partial bar: {e}")
                return None
        return None

    async def seed_historical(self, contract: Contract, timeframe: str, max_tf: str, max_look_back: int) -> Optional[pd.DataFrame]:
        # Heuristic duration string logic (kept; you may simplify later)
        duration_value = int(round(max_look_back * 1.5))
        self.logger.info(f"⌛️ Calculating duration string for seeding {contract.symbol} {timeframe} bars (max_tf: {max_tf})")
        if max_tf.lower() == "1 week":
            division_factor = MAX_TF_TO_LTF.get(max_tf).get(timeframe, 1)
            duration_str_value = int(duration_value / division_factor)
            if duration_str_value == 0 and timeframe == '1 day':
                duration_str_value = 5
            duration_str = f"{duration_str_value} W"
            self.logger.info(f"📅 Duration string set to {duration_str} (weekly)")
        elif max_tf.lower() == "30 mins":
            division_factor = MAX_TF_TO_LTF.get(max_tf).get(timeframe, 1)
            duration_str_value = int(duration_value / division_factor)
            if duration_str_value == 0 and timeframe == '5 mins':
                duration_str_value = 2
            if duration_str_value == 0 and timeframe == '1 min':
                duration_str_value = 2
            duration_str = f"{duration_str_value} D"
            self.logger.info(f"⏱️ Duration string set to {duration_str} (intraday)")
        elif max_tf.lower() == "3 mins":
            division_factor = MAX_TF_TO_LTF.get(max_tf).get(timeframe, 1)
            duration_str_value = int(duration_value / division_factor)
            if duration_str_value == 0 and timeframe == '2 mins':
                duration_str_value = 2
            if duration_str_value == 0 and timeframe == '1 min':
                duration_str_value = 2
            duration_str = f"{duration_str_value} D"
            self.logger.info(f"⏱️ Duration string set to {duration_str} (intraday)")
        else:
            duration_str = f"{duration_value} D"
            self.logger.info(f"⌛️ Using default duration string {duration_str} for {contract.symbol} {timeframe}")

        try:
            await self.ib_connector.ensure_connected()
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

        # Normalize timezone consistently (assume IB returns naive UTC; adjust if needed)
        #df['date'] = pd.to_datetime(df['date'], utc=True)
        df['date'] = pd.to_datetime(df['date'], utc=False)
        df.set_index('date', inplace=True)

        required_cols = ['open', 'high', 'low', 'close', 'volume']
        if not all(col in df.columns for col in required_cols):
            self.logger.error(f"❌ Missing required columns in historical data for {contract.symbol} [{timeframe}]")
            return None

        self.logger.info(f"✅ Seeded {len(df)} bars for {contract.symbol} [{timeframe}]")
        return df

    async def subscribe_live_ticks(self, symbol: str) -> bool:
        await self.ib_connector.ensure_connected()
        if symbol in self.live_tick_subscriptions:
            return True
        contract = self._subscriptions.get(symbol, {}).get('5 secs', {}).get('contract')
        if contract is None:
            self.logger.error(f"🚫 No contract found for {symbol} to subscribe market data")
            return False
        try:
            ticker = self.ib.reqMktData(contract, "", False, False)
            self.tickers[symbol] = ticker
            self.live_tick_subscriptions.add(symbol)
            self.logger.info(f"📡 Subscribed to market data for {symbol}")
            ticker.updateEvent += functools.partial(self._on_ticker_update, symbol=symbol)
            return True
        except Exception as e:
            self.logger.error(f"🚫 Failed to subscribe market data for {symbol}: {e}")
            return False

    async def unsubscribe_live_ticks(self, symbol: str):
        await self.ib_connector.ensure_connected()
        if symbol not in self.live_tick_subscriptions:
            return
        ticker = self.tickers.get(symbol)
        if ticker:
            self.ib.cancelMktData(ticker)
            self.logger.info(f"📴 Unsubscribed market data for {symbol}")
            del self.tickers[symbol]
        self.live_tick_subscriptions.remove(symbol)

    def _on_ticker_update(self, ticker, symbol):
        # last_price = ticker.last
        # self.logger.debug(f"Market data update for {symbol}: Last Price = {last_price}")
        pass

    async def subscribe(self, contract: Contract, timeframe: str, max_tf: str, max_look_back: int) -> bool:
        await self.ib_connector.ensure_connected()
        symbol = contract.symbol

        async with self._lock:
            if timeframe in self._subscriptions[symbol]:
                self.logger.info(f"♻️ Already subscribed: {symbol} [{timeframe}]")
                return True

            # Seed historical
            self.logger.info(f"🌱 Seeding historical data for {symbol} [{timeframe}]")
            df = await self.seed_historical(contract, timeframe, max_tf, max_look_back)
            if df is None:
                df = pd.DataFrame(columns=['open', 'high', 'low', 'close', 'volume'])
                self.logger.warning(f"⚠️ Starting with empty data for {symbol} [{timeframe}] after failed seed")
            self._data_cache[symbol][timeframe] = df

            try:
                # Ensure symbol has 5-sec realtime subscription
                if '5 secs' not in self._subscriptions[symbol]:
                    await self.ib_connector.ensure_connected()
                    bars = self.ib.reqRealTimeBars(contract, 5, "TRADES", useRTH=True)
                    bars.updateEvent += functools.partial(self._on_bar_update, symbol=symbol)
                    self._subscriptions[symbol]['5 secs'] = {'contract': contract, 'bars': bars}
                    self.logger.info(f"📡 Subscribed to real-time 5-sec bars for {symbol}")

                # Ensure aggregators for requested timeframe (and chain)
                self._ensure_aggregator_chain(symbol, timeframe)

                # Register the "subscription" for bookkeeping
                self._subscriptions[symbol][timeframe] = {'contract': contract}
                self.logger.info(f"✅ Subscribed {symbol} [{timeframe}] with aggregator chain ready.")
                self.logger.debug(f"Aggregators now: {list(self._aggregators.keys())}")
                return True

            except Exception as e:
                self.logger.error(f"❌ Failed to subscribe to {symbol} [{timeframe}]: {e}")
                return False

    def _ensure_aggregator_chain(self, symbol: str, target_tf: str):
        """
        Ensure all aggregators needed to produce target_tf exist for this symbol.
        """
        chain: List[Tuple[str, str, int]] = []
        tf = target_tf
        while tf in PARENT_TF:
            parent_tf, count = PARENT_TF[tf]
            chain.append((tf, parent_tf, count))
            tf = parent_tf

        tz = pytz.timezone(self.time_zone)
        for tf, parent_tf, count in reversed(chain):
            key = (symbol, tf)
            if key not in self._aggregators:
                self._aggregators[key] = BarAggregator(
                    target_timeframe=tf,
                    parent_timeframe=parent_tf,
                    count=count,
                    logger=self.logger,
                    tz=tz
                )
                self.logger.info(f"🛠 Created aggregator for {symbol} [{tf}] consuming {count} × [{parent_tf}]")

    async def _on_bar_update(self, bars, hasNewBar, symbol):
        """
        IB realtime 5-second bar update handler for a symbol.
        Always routes from 5 secs up the chain.
        """
        if not hasNewBar:
            return

        latest_bar = bars[-1]
        bar_time = latest_bar.time

        # Normalize time to timezone-aware UTC; downstream can floor to target tz
        if isinstance(bar_time, int):
            bar_time = datetime.fromtimestamp(bar_time, tz=pytz.UTC)
        elif isinstance(bar_time, str):
            bar_time = pd.to_datetime(bar_time, utc=True).to_pydatetime()
        elif getattr(bar_time, "tzinfo", None) is None:
            bar_time = pytz.UTC.localize(bar_time)

        # 5-sec base bar
        base_bar = AggregatedBar(
            time=bar_time,
            open_=latest_bar.open_,
            high=latest_bar.high,
            low=latest_bar.low,
            close=latest_bar.close,
            volume=latest_bar.volume,
        )

        # Update 5-sec cache (no callbacks by default)
        await self._update_cache_and_callbacks(symbol, '5 secs', base_bar, callback=False)

        # Cascade to higher TFs beginning with 1 min if present
        await self._cascade(symbol, '1 min', base_bar)

        # Optional: if you have aggregators that consume directly from 5 secs (custom TFs), you can handle them here.

    async def _cascade(self, symbol: str, target_tf: str, parent_bar: AggregatedBar):
        """
        Feed a parent_bar into the aggregator for target_tf (if it exists).
        If a completed bar results, update cache/callbacks, then propagate to children.
        Otherwise, optionally publish partials (callback=False).
        """
        aggr = self._aggregators.get((symbol, target_tf))
        if not aggr:
            return

        completed = aggr.add_bar(parent_bar)
        if completed:
            await self._update_cache_and_callbacks(symbol, target_tf, completed, callback=True)
            
            # If a 1-min bar just completed, print last rows of other TFs
            if target_tf == '1 min':
                self._print_last_rows_on_1min_complete(symbol)
            
            # Propagate to any children whose parent is target_tf
            for child_tf, (parent_tf, _) in PARENT_TF.items():
                if parent_tf == target_tf:
                    await self._cascade(symbol, child_tf, completed)
        else:
            # Publish partial bar for UI (optional)
            partial = aggr.partial_aggregate_bars()
            if partial:
                await self._update_cache_and_callbacks(symbol, target_tf, partial, callback=False)

    async def _update_cache_and_callbacks(self, symbol: str, timeframe: str, agg_bar: AggregatedBar, callback=True):
        row = {
            'open': float(agg_bar.open_),
            'high': float(agg_bar.high),
            'low': float(agg_bar.low),
            'close': float(agg_bar.close),
            'volume': float(agg_bar.volume)
        }

        df = self._data_cache.get(symbol, {}).get(timeframe)
        if df is None or df.empty:
            df = pd.DataFrame(columns=['open', 'high', 'low', 'close', 'volume'])

        idx_time = agg_bar.time

        # Ensure tz-aware and consistent index timezone (UTC here)
        if isinstance(idx_time, pd.Timestamp):
            idx_time = idx_time.to_pydatetime()
        if getattr(idx_time, "tzinfo", None) is None:
            idx_time = pytz.UTC.localize(idx_time)

        df_new = pd.DataFrame([row], index=[idx_time])

        if not df.empty and idx_time == df.index[-1]:
            # Update in-place last row
            df.iloc[-1] = row
        else:
            if idx_time in df.index:
                self.logger.warning(f"Duplicate timestamp {idx_time} found, replacing existing row")
                df.loc[idx_time] = row
            else:
                df = pd.concat([df, df_new])
                df = df.sort_index()

        if len(df) > self.buffer_limit:
            self.logger.info(f"✂️ Truncating live data to last {self.buffer_limit} bars for {symbol} [{timeframe}]")
            df = df.tail(self.buffer_limit)

        self._data_cache[symbol][timeframe] = df

        # Optional visibility on select TFs
        '''
        if timeframe in ['1 min', '5 mins', '30 mins']:
            cols = ["open", "high", "low", "close", "volume"]
            self.logger.info(f"📊 Last 5 rows for {symbol} [{timeframe}]:\n{df[cols].tail(5)}")
        '''

        if callback and timeframe != '5 secs':
            await self._fire_callbacks(symbol, timeframe, df)
            
    def _print_last_rows_on_1min_complete(self, symbol: str):
        """
        Print/log last N rows from other timeframes whenever a 1-min bar completes.
        Adjust `timeframes_to_show` and `n` as desired.
        """
        timeframes_to_show = ['1 min', '2 mins', '3 mins', '5 mins', '30 mins']
        n = 5
        for tf in timeframes_to_show:
            df = self._data_cache.get(symbol, {}).get(tf)
            if df is not None and not df.empty:
                cols = ["open", "high", "low", "close", "volume"]
                show_cols = [c for c in cols if c in df.columns]
                try:
                    self.logger.info(f"🧾 Last {min(n, len(df))} rows for {symbol} [{tf}] after 1-min complete:\n{df[show_cols].tail(n)}")
                except Exception:
                    # Fallback if column subset fails
                    self.logger.info(f"🧾 Last {min(n, len(df))} rows for {symbol} [{tf}] after 1-min complete:\n{df.tail(n)}")
            else:
                pass
                #self.logger.info(f"🧾 No data yet for {symbol} [{tf}] after 1-min complete.")

    async def _fire_callbacks(self, symbol: str, timeframe: str, df: pd.DataFrame):
        await self.ib_connector.ensure_connected()
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
            await asyncio.gather(*tasks, return_exceptions=True)

    def on_bar(self, symbol: str, timeframe: str, callback: Callable):
        self._callbacks[(symbol, timeframe)].append(callback)
        self.logger.info(f"📝 Registered callback for {symbol} [{timeframe}]")

    def get_latest(self, symbol: str, timeframe: str) -> Optional[pd.DataFrame]:
        return self._data_cache.get(symbol, {}).get(timeframe)

    async def unsubscribe(self, symbol: str):
        await self.ib_connector.ensure_connected()
        async with self._lock:
            subs = self._subscriptions.pop(symbol, {})
            for tf, info in subs.items():
                bars = info.get('bars')
                if bars:
                    try:
                        self.ib.cancelRealTimeBars(bars)
                        self.logger.info(f"🛑 Unsubscribed realtime bars for {symbol} [{tf}]")
                    except Exception as e:
                        self.logger.error(f"❌ Error unsubscribing {symbol} [{tf}]: {e}")

            # Clean tickers
            if symbol in self.tickers:
                try:
                    self.ib.cancelMktData(self.tickers[symbol])
                except Exception as e:
                    self.logger.error(f"❌ Error canceling market data for {symbol}: {e}")
                self.tickers.pop(symbol, None)
            self.live_tick_subscriptions.discard(symbol)

            # Clear cache
            self._data_cache.pop(symbol, None)

            # Remove aggregators for this symbol
            keys_to_remove = [k for k in self._aggregators.keys() if k[0] == symbol]
            for k in keys_to_remove:
                self._aggregators.pop(k, None)

            self.logger.info(f"🗑️ Cleared data cache, aggregators and subscriptions for {symbol}")

    async def close(self):
        await self.ib_connector.ensure_connected()
        async with self._lock:
            symbols = list(self._subscriptions.keys())
            for sym in symbols:
                await self.unsubscribe(sym)
            self.logger.info(f"🛑 Closed StreamingData and unsubscribed all")
        # Shut down executor
        try:
            self.executor.shutdown(wait=False)
        except Exception:
            pass