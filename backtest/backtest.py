import os
import datetime as dt
import pandas as pd
import pytz
import csv
from tqdm import tqdm
import sys
import logging
# Import your strategy, indicators, and helpers
from strategies.scalping_strategy import check_HTF_conditions, check_MTF_conditions, check_LTF_conditions
from execution.position_sizer import compute_qty
from indicators.atr import calculate_atr
from risk_management.atr_based_sl_tp import compute_atr_sl_tp
from risk_management.fixed_sl_tp import compute_fixed_sl_tp
from config.read_ta_settings import read_ta_settings
from utils.sanitize_filenames import sanitize_filename
from utils.ensure_utc import ensure_utc
from utils.time_to_str import time_to_str
from utils.make_path import make_path

class PreMarketChecksBacktest:
    def __init__(self, historical_vix: pd.DataFrame, historical_spx: pd.DataFrame, config_dict: dict, logger):
        self.vix_df = historical_vix
        self.spx_df = historical_spx
        self.config_dict = config_dict
        self.logger = logger
        self.trading_windows = config_dict.get("trading_windows", {})
        self.test_run = config_dict.get("test_run", False)
        self.logger.info("🛠️ Initialized PreMarketChecksBacktest")

    def validate_config(self) -> tuple:
        self.logger.info("🔍 Validating config...")
        if not self.config_dict:
            self.logger.error("❌ Config dict missing")
            return False, "Config dict missing"
        self.logger.info("✅ Config validation passed")
        return True, ""

    def get_close_price(self, df: pd.DataFrame, date: dt.date) -> float:
        try:
            return df.loc[df.index.date == date]['close'].iloc[-1]
        except (IndexError, KeyError):
            self.logger.warning(f"⚠️ No close price found for {date}")
            return None

    def rule_of_16_check(self, date: dt.date) -> tuple:
        self.logger.info(f"🔎 Running Rule-of-16 check for {date}")
        vix_val = self.get_close_price(self.vix_df, date)
        spx_quote = self.get_close_price(self.spx_df, date)
        if vix_val is None or spx_quote is None:
            msg = f"Missing VIX/SPX data for {date}"
            self.logger.warning(f"⚠️ {msg}")
            return False, msg
        expected_daily_move = spx_quote / 16

        if vix_val/1000 > expected_daily_move:
            msg = f"Rule of 16 failed (VIX too high) on {date}"
            self.logger.warning(f"⚠️ {msg}")
            return False, msg
        self.logger.info("✅ Rule-of-16 passed")
        return True, ""

    def in_trading_window(self, date_time: dt.datetime) -> tuple:
        day_name = date_time.strftime('%A').lower()
        self.logger.info(f"⏰ Checking trading window for {day_name} {date_time.time()}")
        if day_name not in self.trading_windows:
            msg = f"No trading window for {day_name}"
            self.logger.warning(f"⚠️ {msg}")
            return False, msg
        start_str, end_str = self.trading_windows[day_name]
        start_time = dt.datetime.strptime(start_str, '%H:%M').time()
        end_time = dt.datetime.strptime(end_str, '%H:%M').time()
        current_time = date_time.time()
        if not (start_time <= current_time <= end_time):
            msg = f"Outside trading window {start_str} - {end_str} on {day_name}"
            self.logger.warning(f"⚠️ {msg}")
            return False, msg
        self.logger.info("✅ Inside trading window")
        return True, ""

    def run_checks_for_day(self, date: dt.date) -> tuple:
        self.logger.info(f"🎯 Running pre-market checks for {date}")
        ok, msg = self.validate_config()
        if not ok:
            return False, f"Config validation failed: {msg}"
        ok, msg = self.rule_of_16_check(date)
        if not ok:
            return False, msg
        if not self.test_run:
            first_window = list(self.trading_windows.values())[0]
            test_time = dt.datetime.combine(date, dt.datetime.strptime(first_window[0], '%H:%M').time())
            ok, msg = self.in_trading_window(test_time)
            if not ok:
                return False, msg
        self.logger.info(f"✅ All pre-market checks passed for {date}")
        return True, ""

    def get_vix(self, date):
        return self.get_close_price(self.vix_df, date)

class BacktestEngine:
    def __init__(self, watchlist, account_value, data_fetcher, config_dict, premarket, logger, commission=0.0005):
        self.watchlist = watchlist
        self.account_value = account_value
        self.commission = commission
        self.positions = {}
        self.trades = []
        self.data_fetcher = data_fetcher
        self.config_dict = config_dict
        self.premarket = premarket
        self.logger = logger
        self.backtest_file_handler = None  # For additional backtest log file handler
        self.logger.info("🛠️ Initialized BacktestEngine")

    def start_backtest_logging(self, backtest_log_path):
        if self.backtest_file_handler is None:
            backtest_handler = logging.FileHandler(backtest_log_path)
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            backtest_handler.setFormatter(formatter)
            self.logger.addHandler(backtest_handler)
            self.backtest_file_handler = backtest_handler
            self.logger.info(f"Backtest logging started in {backtest_log_path}")

    def stop_backtest_logging(self):
        if self.backtest_file_handler:
            self.logger.removeHandler(self.backtest_file_handler)
            self.backtest_file_handler.close()
            self.backtest_file_handler = None
            self.logger.info("Backtest logging stopped")

    def compute_qty(self, price):
        units = int(self.cfg.get("trading_units", 5))
        qty = int(self.account_value * units / price)
        return max(qty, 0)

    def enter_trade(self, symbol, price, qty, sl_price, tp_price, signal, entry_time, side="BUY"):
        if symbol in self.positions and self.positions[symbol]['qty'] != 0:
            self.logger.info(f"Already have active position on {symbol}, skipping entry")
            return

        self.positions[symbol] = {
            'entry_price': price,
            'qty': qty,
            'sl_price': sl_price,
            'tp_price': tp_price,
            'signal': signal,
            'side': side,
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
            'side': side,
            'entry_time': entry_time,
            'exit_time': None,
            'pnl': None
        })
        self.logger.info(f"Entered trade {symbol} qty {qty} entry {price:.2f} SL {sl_price:.2f} TP {tp_price:.2f}")

    def exit_trade(self, symbol, price, time):
        if symbol not in self.positions or self.positions[symbol]['qty'] == 0:
            return

        pos = self.positions[symbol]
        qty = pos['qty']
        entry_price = pos['entry_price']
        gross_pnl = (price - entry_price) * qty if pos['side'] == "BUY" else (entry_price - price) * qty
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

        self.logger.info(f"Exited trade {symbol} qty {qty} at {price:.2f} time {time} P&L: {net_pnl:.2f}")

    def write_daily_checks_to_file(self, daily_checks, filename="daily_pre_checks_report", backtest_directory="backtest_reports"):
        backtest_dir = self.config_dict['backtest_directory'] 
        if not os.path.exists(backtest_dir):
            os.makedirs(backtest_dir)
        file = f'{filename}.csv'
        filepath = make_path(backtest_dir, file)
        with open(filepath, mode='w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["date", "conditions_met", "message"])
            for date, passed, reason in daily_checks:
                writer.writerow([date.strftime("%Y-%m-%d"), passed, reason])
        self.logger.info(f"Daily checks report saved to {filepath}")



        
    async def run_backtest(
        self,
        config_dict,
        symbol_list,
        watchlist_main_settings,
        duration_value=2,
        duration_unit='weeks',
        end_time=None,
        save_data=True,
        load_data=True
    ):
        config_directory = config_dict.get("config_directory", "")
        # Start backtest logging to additional file
        self.start_backtest_logging("backtest_reports/backtest_additional.log")

        self.logger.info("🚀 Starting backtest")
        if end_time is None:
            end_time = dt.datetime.now()
        tz = pytz.timezone(config_dict['trading_time_zone'])

        daily_checks = []
        # for i, symbol in enumerate(tqdm(symbol_list, desc="Backtesting symbols"), 1):
        for i, symbol in enumerate(symbol_list):
            self.logger.info(f"🔄 Processing symbol {i}/{len(symbol_list)}: {symbol}")
            exit_method = watchlist_main_settings[symbol]['Exit']
            exit_sl_input = watchlist_main_settings[symbol]['SL']
            exit_tp_input = watchlist_main_settings[symbol]['TP']

            start_time = self.data_fetcher.get_start_time(end_time, duration_value, duration_unit)
            if start_time.tzinfo is None:
                start_time = tz.localize(start_time)
            if end_time.tzinfo is None:
                end_time = tz.localize(end_time)

            print(start_time, end_time)
            dfs, timeframes = await self.data_fetcher.fetch_multiple_timeframes(symbol, start_time, end_time, save=save_data, load=load_data)
            df_HTF = dfs.get(timeframes[0])
            df_MTF = dfs.get(timeframes[1])
            df_LTF = dfs.get(timeframes[2])

            if df_LTF is None or df_LTF.empty or df_HTF is None or df_MTF is None:
                self.logger.warning(f"⚠️ Skipping {symbol} due to missing data.")
                continue
            unique_days = df_LTF.index.normalize().unique()
            self.logger.info(f"📅 Backtesting {symbol} for {len(unique_days)} trading days")
            for current_day in tqdm(unique_days, desc=f"Days for {symbol}", position=1, leave=False):
                self.logger.info(f'\n📅 Current day ==> {current_day}')
                passed, reason = self.premarket.run_checks_for_day(current_day.date())

                if not any(d[0] == current_day.date() for d in daily_checks):
                    daily_checks.append((current_day.date(), passed, reason))
                if not passed:
                    self.logger.warning(f"⚠️ Pre-market check failed for {symbol} on {current_day.date()}: {reason}")
                    continue
                day_start = current_day
                day_end = current_day + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
                df_LTF_day = df_LTF.loc[(df_LTF.index >= day_start) & (df_LTF.index <= day_end)]
                for idx in tqdm(range(len(df_LTF_day)), desc=f"Processing bars {symbol} {current_day.date()}", position=2, leave=False):

                    df_LTF_slice = df_LTF_day.iloc[:idx + 1]
                    current_time = df_LTF_slice.index[-1]
                    print("current_time ===> ", current_time)
                    print("df_HTF ===> ", df_HTF.head())
                    
                    df_HTF.index = ensure_utc(pd.to_datetime(df_HTF.index))
                    df_MTF.index = ensure_utc(pd.to_datetime(df_MTF.index))
                    
                    current_time_utc = ensure_utc(pd.DatetimeIndex([current_time]))[0]
                    df_HTF_slice = df_HTF.loc[:current_time_utc]            
                    df_MTF_slice = df_MTF.loc[:current_time_utc]
                    try:
                        self.logger.info(
                            f"df_HTF_slice --> {df_HTF_slice} | "
                            f"df_MTF_slice --> {df_MTF_slice} | "
                            f"df_LTF_slice --> {df_LTF_slice}"
                        )
                    except Exception as e:
                        self.logger.info(f"Exception: {e}")

                    ta_settings, max_look_back = read_ta_settings(symbol, config_directory, self.logger)
                    okHTF = check_HTF_conditions(symbol, watchlist_main_settings, ta_settings, max_look_back, df_HTF_slice, self.logger)
                    okMTF = check_MTF_conditions(symbol, watchlist_main_settings, ta_settings, max_look_back, df_MTF_slice, self.logger)
                    if not (okHTF and okMTF):
                        continue
                    if len(df_LTF_slice) < 10:
                        continue
                    sig = check_LTF_conditions(symbol, watchlist_main_settings, ta_settings, max_look_back, df_LTF_slice, df_HTF_slice, self.logger)
                    if sig and (symbol not in self.positions or self.positions[symbol]['qty'] == 0):
                        last_price = float(df_LTF_slice['close'].iloc[-1])
                        vix = self.premarket.get_close_price(self.premarket.vix_df, current_day.date())
                        qty = compute_qty(
                            self.account_value,
                            units=int(self.cfg.get("trading_units", 5)),
                            price=last_price,
                            vix=vix,
                            vix_threshold=float(self.cfg.get("vix_threshold", 20)),
                            vix_reduction_factor=float(self.cfg.get("vix_reduction_factor", 1)),
                            skip_on_high_vix=bool(self.cfg.get("skip_on_high_vix", False)),
                        )
                        if qty <= 0:
                            continue

                        if exit_method == "E1":
                            sl_price, tp_price = compute_fixed_sl_tp(
                                last_price,
                                sl_pct=exit_sl_input,
                                tp_pct=exit_tp_input
                            )
                        else:  # ATR
                            try:
                                atr_val = float(calculate_atr(df_LTF_slice, 14).iloc[-1])
                            except Exception:
                                atr_val = None
                            if atr_val is not None:
                                sl_price, tp_price = compute_atr_sl_tp(
                                    last_price,
                                    atr_val,
                                    k_sl=exit_sl_input,
                                    k_tp=exit_tp_input
                                )
                            else:
                                sl_price, tp_price = compute_fixed_sl_tp(
                                    last_price,
                                    sl_pct=exit_sl_input,
                                    tp_pct=exit_tp_input
                                )
                        entry_time = current_time
                        self.enter_trade(symbol, last_price, qty, sl_price, tp_price, sig, entry_time, side="BUY")
            self.logger.info(f"Backtest complete for {symbol}")



        backtest_dir = config_dict['backtest_directory']  
        now = dt.datetime.now()
        timestamp_str = time_to_str(now, only_date=True)
        start_str = time_to_str(start_time, only_date=True)
        end_str = time_to_str(end_time, only_date=True)
        backtest_file = f"{symbol}_{timestamp_str}_{start_str}_{end_str}"      
        daily_checks_file = f"daily_pre_checks_report_{timestamp_str}_{start_str}_{end_str}"
        self.write_daily_checks_to_file(daily_checks, daily_checks_file)
        self.report_results()
        self.write_trades_to_file(backtest_dir, f"{backtest_file}_backtest_reports")

        # Stop backtest logging to additional file after backtest finishes
        self.stop_backtest_logging()

    def report_results(self):
        total_trades = len(self.trades)
        winning_trades = [t for t in self.trades if t['pnl'] and t['pnl'] > 0]
        losing_trades = [t for t in self.trades if t['pnl'] and t['pnl'] <= 0]
        total_pnl = sum(t['pnl'] for t in self.trades if t['pnl'] is not None)
        win_rate = len(winning_trades) / total_trades if total_trades > 0 else 0

        self.logger.info("Backtest summary:")
        self.logger.info(f"Total trades: {total_trades}")
        self.logger.info(f"Winning trades: {len(winning_trades)}")
        self.logger.info(f"Losing trades: {len(losing_trades)}")
        self.logger.info(f"Win rate: {win_rate*100:.2f}%")
        self.logger.info(f"Net P&L: {total_pnl:.2f}")
        print("Backtest summary:")
        print(f"Total trades: {total_trades}")
        print(f"Winning trades: {len(winning_trades)}")
        print(f"Losing trades: {len(losing_trades)}")
        print(f"Win rate: {win_rate*100:.2f}%")
        print(f"Net P&L: {total_pnl:.2f}")

    def write_trades_to_file(self, backtest_dir, file):
       
        if not os.path.exists(backtest_dir):
            os.makedirs(backtest_dir)

        filepath = make_path(backtest_dir, f'{file}.csv')

        cum_pnl = 0.0
        rows = []
        for trade in self.trades:
            if trade['pnl'] is None:
                continue
            cum_pnl += trade['pnl']
            symbol = trade['symbol']
            side = 'long' if trade.get('side', 'BUY').upper() == 'BUY' else 'short'
            entry_dt = trade['entry_time']
            exit_dt = trade['exit_time'] or ''

            entry_date = entry_dt.strftime('%Y-%m-%d') if isinstance(entry_dt, (dt.datetime, pd.Timestamp)) else ''
            entry_time = entry_dt.strftime('%H:%M:%S') if isinstance(entry_dt, (dt.datetime, pd.Timestamp)) else ''
            exit_date = exit_dt.strftime('%Y-%m-%d') if isinstance(exit_dt, (dt.datetime, pd.Timestamp)) else ''
            exit_time = exit_dt.strftime('%H:%M:%S') if isinstance(exit_dt, (dt.datetime, pd.Timestamp)) else ''

            row = [
                symbol,
                side,
                entry_date,
                entry_time,
                f"{trade['entry_price']:.4f}",
                exit_date,
                exit_time,
                f"{trade['exit_price']:.4f}" if trade['exit_price'] is not None else '',
                f"{trade['pnl']:.4f}",
                f"{cum_pnl:.4f}"
            ]
            rows.append(row)

        with open(filepath, mode='w', newline='') as f:
            writer = csv.writer(f)
            header = [
                'symbol',
                'long/short',
                'entry_date',
                'entry_time',
                'entry_price',
                'exit_date',
                'exit_time',
                'exit_price',
                'profit/loss',
                'cum_profit_loss'
            ]
            writer.writerow(header)
            writer.writerows(rows)

        self.logger.info(f"Backtest trades report saved to {filepath}")
