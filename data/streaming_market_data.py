import asyncio
import functools
import logging
from collections import defaultdict, deque
from typing import Dict, Callable, Optional, List, Tuple
import pandas as pd
from datetime import datetime
from dataclasses import dataclass
from ib_async import IB, Contract, util
from concurrent.futures import ThreadPoolExecutor

TF_TO_MINUTES = {
    '1 min': 1,
    '5 mins': 5,
    '30 mins': 30,
    '1 day': 60 * 24,
    '1 week': 60 * 24 * 7
}

MAX_TF_TO_LTF = {
    '1 week': {'1 week': 1, '1 day': 5, '5 mins': 78 * 5},
    '30 mins': {'30 mins': 1, '5 mins': 6, '1 min': 30}
}

@dataclass
class AggregatedBar:
    time: datetime
    open_: float
    high: float
    low: float
    close: float
    volume: float

class BarAggregator:
    def __init__(self, bars_to_aggregate: int, logger):
        self.bars_to_aggregate = bars_to_aggregate
        self.logger = logger
        self.bars = deque()  # Store incoming smaller bars (e.g., 5s bars for 1min aggr)

    def add_bar(self, bar: AggregatedBar) -> Optional[AggregatedBar]:
        self.bars.append(bar)
        if len(self.bars) == self.bars_to_aggregate:
            agg_bar = self.aggregate_bars(list(self.bars))
            self.bars.clear()
            self.logger.debug(f"Aggregated {self.bars_to_aggregate} bars at {agg_bar.time}")
            return agg_bar
        return None

    def partial_aggregate_bars(self) -> Optional[AggregatedBar]:
        if not self.bars:
            return None
        open_ = self.bars[0].open_
        high = max(b.high for b in self.bars)
        low = min(b.low for b in self.bars)
        close = self.bars[-1].close
        volume = sum(b.volume for b in self.bars)
        last_time = self.bars[-1].time

        time = self._normalize_time(last_time)

        return AggregatedBar(time, open_, high, low, close, volume)

    def aggregate_bars(self, bars: List[AggregatedBar]) -> AggregatedBar:
        open_ = bars[0].open_
        high = max(b.high for b in bars)
        low = min(b.low for b in bars)
        close = bars[-1].close
        volume = sum(b.volume for b in bars)
        last_time = bars[-1].time

        time = self._normalize_time(last_time)

        return AggregatedBar(time, open_, high, low, close, volume)

    def _normalize_time(self, dt: datetime) -> datetime:
        """
        Normalize the timestamp to the nearest lower multiple of aggregation interval in minutes.
        For example, for 5-min aggregation, normalize 08:27 to 08:25.
        """
        total_minutes = dt.hour * 60 + dt.minute
        rounded_minutes = total_minutes - (total_minutes % self.bars_to_aggregate)
        new_hour = rounded_minutes // 60
        new_minute = rounded_minutes % 60
        return dt.replace(hour=new_hour, minute=new_minute, second=0, microsecond=0)

class StreamingData:
    def __init__(self, ib: IB, logger: logging.Logger, trading_time_zone, buffer_limit: int = 1000):
        self.ib = ib
        self.logger = logger
        self.time_zone = trading_time_zone
        self.buffer_limit = buffer_limit
        self.executor = ThreadPoolExecutor(max_workers=5)
        self._lock = asyncio.Lock()
        self._subscriptions: Dict[str, Dict[str, dict]] = defaultdict(dict)  # symbol -> timeframe -> subscription info
        self._data_cache: Dict[str, Dict[str, pd.DataFrame]] = defaultdict(dict)  # symbol -> timeframe -> df
        self._callbacks: Dict[Tuple[str, str], List[Callable]] = defaultdict(list)
        self._aggregators: Dict[Tuple[str, str], BarAggregator] = {}  # (symbol, timeframe) -> BarAggregator
        self.logger.info(f"🚀 StreamingData initialized with buffer limit {buffer_limit} bars.")

    async def seed_historical(self, contract: Contract, timeframe: str, max_tf: str, max_look_back: int) -> Optional[pd.DataFrame]:
        duration_value = int(round(max_look_back * 1.5))
        self.logger.info(f"⌛️ Calculating duration string for seeding {contract.symbol} {timeframe} bars (max_tf: {max_tf})")
        if max_tf.lower() == "1 week":
            division_factor = MAX_TF_TO_LTF.get(max_tf).get(timeframe)
            duration_str_value = int(duration_value / division_factor)
            if duration_str_value == 0 and timeframe == '1 day':
                duration_str_value = 5
            duration_str = f"{duration_str_value} W"
            self.logger.info(f"📅 Duration string set to {duration_str} (weekly)")
        elif max_tf.lower() == "30 mins":
            division_factor = MAX_TF_TO_LTF.get(max_tf).get(timeframe)
            duration_str_value = int(duration_value / division_factor)
            if duration_str_value == 0 and timeframe == '1 min':
                duration_str_value = 2
            duration_str = f"{duration_str_value} D"
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

        required_cols = ['open', 'high', 'low', 'close', 'volume']
        if not all(col in df.columns for col in required_cols):
            self.logger.error(f"❌ Missing required columns in historical data for {contract.symbol} [{timeframe}]")
            return None

        self.logger.info(f"✅ Seeded {len(df)} bars for {contract.symbol} [{timeframe}]")
        return df

    async def subscribe(self, contract: Contract, timeframe: str, max_tf: str, max_look_back: int) -> bool:
        symbol = contract.symbol
        async with self._lock:
            if timeframe in self._subscriptions[symbol]:
                self.logger.info(f"♻️ Already subscribed: {symbol} [{timeframe}]")
                return True
            
            self.logger.info(f"🌱 Seeding historical data for {symbol} [{timeframe}]")
            df = await self.seed_historical(contract, timeframe, max_tf, max_look_back)
            if df is None:
                df = pd.DataFrame(columns=['open', 'high', 'low', 'close', 'volume'])
                self.logger.warning(f"⚠️ Starting with empty data for {symbol} [{timeframe}] after failed seed")
            
            self._data_cache[symbol][timeframe] = df
            try:
                if timeframe == '1 min':
                    bars = self.ib.reqRealTimeBars(contract, 5, "TRADES", useRTH=True)
                    bars.updateEvent += functools.partial(self._on_bar_update, symbol=symbol, timeframe='5 secs')
                    self._subscriptions[symbol]['5 secs'] = {'contract': contract, 'bars': bars}
                    self.logger.info(f"📡 Subscribed to real-time 5-sec bars for {symbol}")
                    self._aggregators[(symbol, '1 min')] = BarAggregator(12, self.logger)
                else:
                    tf_minutes = TF_TO_MINUTES.get(timeframe)
                    if tf_minutes is None:
                        raise ValueError(f"Unsupported timeframe {timeframe} for aggregation")
                    self._aggregators[(symbol, timeframe)] = BarAggregator(tf_minutes, self.logger)
                    self._subscriptions[symbol][timeframe] = {'contract': contract, 'aggregator': self._aggregators[(symbol, timeframe)]}
                    self.logger.info(f"Setup aggregator for {symbol} [{timeframe}] with bars_to_aggregate={tf_minutes}")
                return True
            except Exception as e:
                self.logger.error(f"❌ Failed to subscribe to {symbol} [{timeframe}]: {e}")
                return False



    async def _on_bar_update(self, bars, hasNewBar, symbol, timeframe):
        if not hasNewBar:
            return
        latest_bar = bars[-1]


        bar_time = latest_bar.time
        if isinstance(bar_time, int):
            bar_time = datetime.fromtimestamp(bar_time)
        elif isinstance(bar_time, str):
            bar_time = pd.to_datetime(bar_time)
        
        # Convert to local timezone (system or specified), e.g., Asia/Kolkata (UTC+5:30)
        import pytz
        local_tz = pytz.timezone(self.time_zone)
        if bar_time.tzinfo is None:
            bar_time = bar_time.replace(tzinfo=pytz.UTC).astimezone(local_tz)
        else:
            bar_time = bar_time.astimezone(local_tz)
        
        bar_time = bar_time.replace(second=(bar_time.second // 5) * 5, microsecond=0)
        base_bar = AggregatedBar(
            time=bar_time,
            open_=latest_bar.open_,
            high=latest_bar.high,
            low=latest_bar.low,
            close=latest_bar.close,
            volume=latest_bar.volume,
        )

        await self._update_cache_and_callbacks(symbol, '5 secs', base_bar, callback=True)

        aggregator_1min = self._aggregators.get((symbol, '1 min'))
        if aggregator_1min:
            agg_bar = aggregator_1min.add_bar(base_bar)
            if agg_bar:
                await self._update_cache_and_callbacks(symbol, '1 min', agg_bar, callback=True)
                for (sym, tf), agg in self._aggregators.items():
                    if sym == symbol and tf not in ['1 min', '5 secs']:
                        agg_bar_higher = agg.add_bar(agg_bar)
                        if agg_bar_higher:
                            await self._update_cache_and_callbacks(symbol, tf, agg_bar_higher, callback=True)
                        else:
                            partial = agg.partial_aggregate_bars()
                            if partial:
                                await self._update_cache_and_callbacks(symbol, tf, partial, callback=False)
            else:
                partial_bar = aggregator_1min.partial_aggregate_bars()
                if partial_bar:
                    await self._update_cache_and_callbacks(symbol, '1 min', partial_bar, callback=False)

    async def _update_cache_and_callbacks(self, symbol: str, timeframe: str, agg_bar: AggregatedBar, callback=True):
        row = {
            'open': agg_bar.open_,
            'high': agg_bar.high,
            'low': agg_bar.low,
            'close': agg_bar.close,
            'volume': agg_bar.volume,
        }
        df = self._data_cache.get(symbol, {}).get(timeframe)
        if df is None:
            df = pd.DataFrame(columns=['open', 'high', 'low', 'close', 'volume'])
        
        df_new = pd.DataFrame([row], index=[agg_bar.time])
        
        if not df.empty and agg_bar.time == df.index[-1]:
            df.iloc[-1] = row
        else:
            df = pd.concat([df, df_new])
        
        if len(df) > self.buffer_limit:
            self.logger.info(f"✂️ Truncating live data to last {self.buffer_limit} bars for {symbol} [{timeframe}]")
            df = df.tail(self.buffer_limit)
        
        self._data_cache[symbol][timeframe] = df

        if callback and timeframe != '5 secs':
            self.logger.info(f"📊 Last 5 rows for {symbol} [{timeframe}]:\n{df.tail(5)}")
            self.logger.info(f"🆕 Updated {timeframe} bar for {symbol} at {agg_bar.time}")
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
                bars = info.get('bars')
                if bars:
                    try:
                        self.ib.cancelRealTimeBars(bars)
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
            self.logger.info(f"🛑 Closed StreamingData and unsubscribed all")
