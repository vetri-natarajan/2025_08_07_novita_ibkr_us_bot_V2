import pandas as pd
import asyncio
import os
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from ib_insync import IB, Stock

class HistoricalDataFetcher:
    def __init__(self, ib: IB, data_dir='historical_data'):
        self.ib = ib
        self.max_days_per_request = 30  # IB max approx for 1-min+ bars
        self.data_dir = data_dir
        os.makedirs(data_dir, exist_ok=True)

    def parse_duration(self, duration_value: int, duration_unit: str):
        duration_unit = duration_unit.lower()
        if duration_unit in ['year', 'years']:
            return relativedelta(years=duration_value)
        elif duration_unit in ['month', 'months']:
            return relativedelta(months=duration_value)
        elif duration_unit in ['week', 'weeks']:
            return timedelta(weeks=duration_value)
        elif duration_unit in ['day', 'days']:
            return timedelta(days=duration_value)
        elif duration_unit in ['min', 'minute', 'minutes']:
            return timedelta(minutes=duration_value)
        else:
            raise ValueError(f"Unsupported duration unit: {duration_unit}")

    def get_start_time(self, end_time: datetime, duration_value: int, duration_unit: str):
        delta = self.parse_duration(duration_value, duration_unit)
        return end_time - delta

    def get_filepath(self, symbol, timeframe, start_time, end_time):
        fname = f"{symbol}_{timeframe}_{start_time.strftime('%Y%m%d')}_{end_time.strftime('%Y%m%d')}.parquet"
        return os.path.join(self.data_dir, fname)

    def load_data(self, symbol, timeframe, start_time, end_time):
        filepath = self.get_filepath(symbol, timeframe, start_time, end_time)
        if os.path.exists(filepath):
            return pd.read_parquet(filepath)
        return None

    def save_data(self, df, symbol, timeframe, start_time, end_time):
        filepath = self.get_filepath(symbol, timeframe, start_time, end_time)
        df.to_parquet(filepath)

    async def fetch_paginated_data(self, symbol: str, timeframe: str, start_time: datetime, end_time: datetime) -> pd.DataFrame:
        contract = Stock(symbol, 'SMART', 'USD')
        bars = []

        current_end = end_time
        max_delta = timedelta(days=self.max_days_per_request)
        bar_size_map = {
            # IB bar size settings
            '1 min': '1 min',
            '5 min': '5 mins',
            '30 min': '30 mins',
            '1 hour': '1 hour',
            '1 day': '1 day',
            '1 week': '1 week',
            '1 month': '1 month'
        }
        bar_size_setting = bar_size_map.get(timeframe.lower(), timeframe)

        while current_end > start_time:
            current_start = max(start_time, current_end - max_delta)

            end_str = current_end.strftime('%Y%m%d %H:%M:%S')
            duration_days = (current_end - current_start).days + 1
            duration_str = f"{duration_days} D"

            try:
                partial_bars = await self.ib.reqHistoricalDataAsync(
                    contract,
                    endDateTime=end_str,
                    durationStr=duration_str,
                    barSizeSetting=bar_size_setting,
                    whatToShow='TRADES',
                    useRTH=True,
                    formatDate=1
                )
            except Exception as e:
                print(f"Error fetching {symbol} {timeframe} from {current_start} to {current_end}: {e}")
                break

            if not partial_bars:
                print(f"No data returned for {symbol} {timeframe} for {current_start} to {current_end}")
                break

            bars.extend(partial_bars)

            current_end = current_start - timedelta(seconds=1)  # move back to avoid overlap

            await asyncio.sleep(0.2)  # slight delay to avoid hitting IB limits

        if not bars:
            return pd.DataFrame()

        df = self.ib.util.df(bars)
        df['date'] = pd.to_datetime(df['date'])
        df = df.drop_duplicates(subset=['date'])
        df = df.set_index('date')
        df = df.loc[~df.index.duplicated(keep='first')]
        df = df.sort_index()
        df = df[(df.index >= start_time) & (df.index <= end_time)]
        return df

    async def fetch_multiple_timeframes(self, symbol, timeframes, start_time, end_time, save=True, load=True):
        dfs = {}
        for tf in timeframes:
            df = None
            if load:
                df = self.load_data(symbol, tf, start_time, end_time)
            if df is None or df.empty:
                df = await self.fetch_paginated_data(symbol, tf, start_time, end_time)
                if save and not df.empty:
                    self.save_data(df, symbol, tf, start_time, end_time)
            dfs[tf] = df
        return dfs


class BacktestEngine:
    def __init__(self, watchlist, account_value, cfg, data_fetcher, commission=0.0005):
        self.watchlist = watchlist
        self.account_value = account_value
        self.cfg = cfg
        self.commission = commission
        self.positions = {}
        self.trades = []
        self.data_fetcher = data_fetcher

    def compute_qty(self, price):
        units = int(self.cfg['TRADING'].get('units', 5))
        qty = int(self.account_value * units / price)
        return max(qty, 0)

    def enter_trade(self, symbol, price, qty, sl_price, tp_price, signal, entry_time):
        if symbol in self.positions and self.positions[symbol]['qty'] != 0:
            print(f"Already have active position on {symbol}, skipping entry")
            return
        
        self.positions[symbol] = {
            'entry_price': price,
            'qty': qty,
            'sl_price': sl_price,
            'tp_price': tp_price,
            'signal': signal,
            'exit_price': None,
            'exit_time': None,
            'pnl': None
        }
        self.trades.append({
            'symbol': symbol,
            'qty': qty,
            'entry_price': price,
            'sl_price': sl_price,
            'tp_price': tp_price,
            'signal': signal,
            'entry_time': entry_time,
            'exit_time': None,
            'pnl': None
        })
        print(f"Entered trade {symbol} qty {qty} entry {price:.2f} SL {sl_price:.2f} TP {tp_price:.2f}")

    def exit_trade(self, symbol, price, time):
        if symbol not in self.positions or self.positions[symbol]['qty'] == 0:
            return
        
        pos = self.positions[symbol]
        qty = pos['qty']
        entry_price = pos['entry_price']

        gross_pnl = (price - entry_price) * qty
        commission_cost = price * qty * self.commission
        net_pnl = gross_pnl - commission_cost

        pos['exit_price'] = price
        pos['exit_time'] = time
        pos['pnl'] = net_pnl
        pos['qty'] = 0
        
        for trade in self.trades:
            if trade['symbol'] == symbol and trade['pnl'] is None:
                trade['exit_price'] = price
                trade['exit_time'] = time
                trade['pnl'] = net_pnl
        
        print(f"Exited trade {symbol} qty {qty} at {price:.2f} time {time} P&L: {net_pnl:.2f}")

    async def run_backtest(self, timeframes, duration_value=2, duration_unit='weeks', end_time=None, save_data=True, load_data=True):
        if end_time is None:
            end_time = datetime.now()

        for symbol in self.watchlist:
            start_time = self.data_fetcher.get_start_time(end_time, duration_value, duration_unit)
            dfs = await self.data_fetcher.fetch_multiple_timeframes(
                symbol, timeframes, start_time, end_time, save=save_data, load=load_data)
            
            print(f"Starting backtest on {symbol}")

            # For demonstration, use 1st timeframe as LTF (most granular)
            ltf = sorted(timeframes, key=lambda x: 
                         pd.Timedelta(x if 'min' in x else '1D'))[0]
            ltf_df = dfs[ltf]
            
            if ltf_df is None or ltf_df.empty:
                print(f"No data for {symbol} timeframe {ltf}")
                continue

            # Here you would implement cross-timeframe logic using dfs
            for idx in range(len(ltf_df)):
                ltf_slice = ltf_df.iloc[:idx+1]
                
                # Replace the following with your own trading logic
                # Multi-timeframe confirmation using dfs dictionary

                # Dummy signal example:
                sig = self.cfg['STRATEGY'].get('dummy_signal', True)
                if sig and (symbol not in self.positions or self.positions[symbol]['qty'] == 0):
                    last_price = ltf_slice['close'].iloc[-1]
                    qty = self.compute_qty(last_price)
                    if qty == 0:
                        continue

                    sl_price = last_price * 0.99  # Example 1% stop loss
                    tp_price = last_price * 1.02  # Example 2% take profit

                    entry_time = ltf_slice.index[-1]
                    self.enter_trade(symbol, last_price, qty, sl_price, tp_price, sig, entry_time)

                if symbol in self.positions and self.positions[symbol]['qty'] != 0:
                    current_price = ltf_slice['close'].iloc[-1]
                    pos = self.positions[symbol]
                    if current_price <= pos['sl_price'] or current_price >= pos['tp_price']:
                        exit_time = ltf_slice.index[-1]
                        self.exit_trade(symbol, current_price, exit_time)

            print(f"Backtest complete for {symbol}")

        self.report_results()

    def report_results(self):
        total_trades = len(self.trades)
        winning_trades = [t for t in self.trades if t['pnl'] and t['pnl'] > 0]
        losing_trades = [t for t in self.trades if t['pnl'] and t['pnl'] <= 0]
        total_pnl = sum(t['pnl'] for t in self.trades if t['pnl'] is not None)
        win_rate = len(winning_trades) / total_trades if total_trades > 0 else 0
        
        print("Backtest summary:")
        print(f"Total trades: {total_trades}")
        print(f"Winning trades: {len(winning_trades)}")
        print(f"Losing trades: {len(losing_trades)}")
        print(f"Win rate: {win_rate*100:.2f}%")
        print(f"Net P&L: {total_pnl:.2f}")
