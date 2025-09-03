import asyncio
import functools
import logging
from collections import defaultdict, deque
from typing import Dict, Callable, Optional, List, Tuple
import pandas as pd
from datetime import datetime, timezone
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
    time: pd.Timestamp
    open_: float
    high: float
    low: float
    close: float
    volume: float

class BarAggregator:
    def __init__(self, timeframe_minutes: int, logger: logging.Logger):
        self.tf = timeframe_minutes
        self.logger = logger
        self.bars = deque()  # Store incoming 1-min bars

    def add_bar(self, bar: AggregatedBar) -> Optional[AggregatedBar]:
        self.bars.append(bar)
        if len(self.bars) == self.tf:
            agg_bar = self.aggregate_bars(list(self.bars))
            self.bars.clear()
            self.logger.debug(f"Aggregated {self.tf}-minute bar at {agg_bar.time}")
            return agg_bar
        return None

    def aggregate_bars(self, bars: List[AggregatedBar]) -> AggregatedBar:
        open_ = bars[0].open_
        high = max(b.high for b in bars)
        low = min(b.low for b in bars)
        close = bars[-1].close
        volume = sum(b.volume for b in bars)
        time = bars[-1].time  # End time of the aggregated bar
        return AggregatedBar(time, open_, high, low, close, volume)

class StreamingData:
    def __init__(self, ib: IB, logger: logging.Logger, buffer_limit: int = 1000):
        self.ib = ib
        self.logger = logger
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
                    bars.updateEvent += functools.partial(self._on_bar_update, symbol=symbol, timeframe='1 min')
                    self._subscriptions[symbol]['1 min'] = {'contract': contract, 'bars': bars}
                    self.logger.info(f"📡 Subscribed to real-time 1-min bars for {symbol}")
                else:
                    tf_minutes = TF_TO_MINUTES.get(timeframe)
                    if tf_minutes is None:
                        raise ValueError(f"Unsupported timeframe {timeframe} for aggregation")

                    self._aggregators[(symbol, timeframe)] = BarAggregator(tf_minutes, self.logger)
                    self._subscriptions[symbol][timeframe] = {'contract': contract, 'aggregator': self._aggregators[(symbol, timeframe)]}
                    self.logger.info(f"Setup aggregator for {symbol} [{timeframe}]")
                return True
            except Exception as e:
                self.logger.error(f"❌ Failed to subscribe to {symbol} [{timeframe}]: {e}")
                return False

    async def _on_bar_update(self, bars, hasNewBar, symbol, timeframe):
        if not hasNewBar or timeframe != "1 min":
            return

        latest_bar = bars[-1]
        bar_time = latest_bar.time

        if isinstance(bar_time, int):
            bar_time = datetime.fromtimestamp(bar_time, timezone.utc)
        elif isinstance(bar_time, str):
            bar_time = pd.to_datetime(bar_time)

        if bar_time.tzinfo is None:
            bar_time = bar_time.tz_localize(timezone.utc)

        bar_time = bar_time.replace(second=0, microsecond=0)

        base_bar = AggregatedBar(
            time=bar_time,
            open_=latest_bar.open_,
            high=latest_bar.high,
            low=latest_bar.low,
            close=latest_bar.close,
            volume=latest_bar.volume
        )

        # Update 1-min cache and callbacks directly
        await self._update_cache_and_callbacks(symbol, "1 min", base_bar)

        # Aggregate for higher timeframes and update if a new bar completes
        for (sym, tf), aggregator in self._aggregators.items():
            if sym != symbol:
                continue

            agg_bar = aggregator.add_bar(base_bar)
            if agg_bar:
                await self._update_cache_and_callbacks(symbol, tf, agg_bar)

    async def _update_cache_and_callbacks(self, symbol: str, timeframe: str, agg_bar: AggregatedBar):
        row = {
            'open': agg_bar.open_,
            'high': agg_bar.high,
            'low': agg_bar.low,
            'close': agg_bar.close,
            'volume': agg_bar.volume
        }

        df = self._data_cache.get(symbol, {}).get(timeframe)
        if df is None:
            df = pd.DataFrame(columns=['open', 'high', 'low', 'close', 'volume'])

        df_new = pd.DataFrame([row], index=[agg_bar.time])
        df = pd.concat([df, df_new])
        df = df[~df.index.duplicated(keep='last')]

        if len(df) > self.buffer_limit:
            self.logger.info(f"✂️ Truncating live data to last {self.buffer_limit} bars for {symbol} [{timeframe}]")
            df = df.tail(self.buffer_limit)

        self._data_cache[symbol][timeframe] = df
        self.logger.info(f"🆕 New {timeframe} bar for {symbol} at {agg_bar.time}")

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
