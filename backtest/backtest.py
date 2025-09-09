import os
import datetime as dt
import pandas as pd
import pytz
import csv
import numpy as np
from tqdm import tqdm
import logging


from risk_management.atr_based_sl_tp import compute_atr_sl_tp
from risk_management.fixed_sl_tp import compute_fixed_sl_tp
from risk_management.dynamic_sl_tp import compute_dynamic_sl_swing
from risk_management.hedge_sl_tp import compute_hedge_exit_trade
from indicators.vwap import calculate_vwap
from config.read_ta_settings import read_ta_settings
from utils.sanitize_filenames import sanitize_filename
from utils.ensure_utc import ensure_utc
from utils.time_to_str import time_to_str
from utils.make_path import make_path
from utils.is_in_trading_window import is_time_in_trading_windows
from utils.ensure_directory import ensure_directory
from strategies.strategy import check_HTF_conditions, check_MTF_conditions, check_LTF_conditions
from execution.position_sizer import compute_qty
from indicators.atr import calculate_atr

class PreMarketChecksBacktest:
    def __init__(self, historical_vix: pd.DataFrame, historical_spx: pd.DataFrame, config_dict: dict, logger):
        self.vix_df = historical_vix
        self.spx_df = historical_spx
        self.config_dict = config_dict
        self.logger = logger
        self.trading_windows = config_dict.get("trading_windows", {})
        self.test_run = config_dict.get("test_run", False)
        self.logger.info("🛠️ Initialized PreMarketChecksBacktest")
        self.skip_backtest_vix = config_dict["skip_backtest_vix"]

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
        
        if self.skip_backtest_vix:  # for testing only
            return True, ""
        elif vix_val > expected_daily_move:
            msg = f"Rule of 16 failed (VIX too high) on {date}"
            self.logger.warning(f"⚠️ {msg}")
            return False, msg
        self.logger.info("✅ Rule-of-16 passed")
        return True, ""

    def run_checks_for_day(self, date: dt.date) -> tuple:
        self.logger.info(f"🎯 Running pre-market checks for {date}")
        ok, msg = self.validate_config()
        if not ok:
            return False, f"Config validation failed: {msg}"
        ok, msg = self.rule_of_16_check(date)
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
        self.backtest_file_handler = None
        self.logger.info("🛠️ Initialized BacktestEngine")

    def start_backtest_logging(self, backtest_log_path):
        if self.backtest_file_handler is None:
            backtest_handler = logging.FileHandler(backtest_log_path)
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            backtest_handler.setFormatter(formatter)
            self.logger.addHandler(backtest_handler)
            self.backtest_file_handler = backtest_handler
            self.logger.info(f"🚀 Backtest logging started in {backtest_log_path}")

    def stop_backtest_logging(self):
        if self.backtest_file_handler:
            self.logger.removeHandler(self.backtest_file_handler)
            self.backtest_file_handler.close()
            self.backtest_file_handler = None
            self.logger.info("⏹️ Backtest logging stopped")

    def compute_qty(self, price):
        units = int(self.config_dict.get("trading_units", 5))
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
            'pnl': None,
            'entry_time': entry_time,
            'profit_taken': False
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

    def exit_trade_partial(self, symbol, price, time, qty_to_exit):
        if symbol not in self.positions or self.positions[symbol]['qty'] == 0:
            return
        pos = self.positions[symbol]

        if qty_to_exit > pos['qty']:
            qty_to_exit = pos['qty']

        entry_price = pos['entry_price']
        side = pos['side']
        qty_remaining = pos['qty'] - qty_to_exit

        gross_pnl = (price - entry_price) * qty_to_exit if side == "BUY" else (entry_price - price) * qty_to_exit
        commission_cost = price * qty_to_exit * self.commission
        net_pnl = gross_pnl - commission_cost

        pos['qty'] = qty_remaining

        if qty_remaining == 0:
            pos['exit_price'] = price
            pos['exit_time'] = time
            pos['pnl'] = net_pnl
        else:
            pos['exit_price'] = None
            pos['exit_time'] = None
            pos['pnl'] = None

        self.trades.append({
            'symbol': symbol,
            'qty': qty_to_exit,
            'entry_price': entry_price,
            'sl_price': pos['sl_price'],
            'tp_price': pos['tp_price'],
            'signal': pos['signal'],
            'side': side,
            'entry_time': pos['entry_time'],
            'exit_time': time,
            'exit_price': price,
            'pnl': net_pnl
        })

        self.logger.info(f"Partially exited trade {symbol} qty {qty_to_exit} at {price:.2f} time {time} P&L: {net_pnl:.2f}")

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
        backtest_dir = config_dict['backtest_directory']
        check_directory = ensure_directory(backtest_dir, self.logger)
        now = dt.datetime.now()
        timestamp_str = time_to_str(now, only_date=True)
        backtest_additional_log_file = f"backtest_additional_{timestamp_str}.log"
        backtest_additional_log_file = make_path(backtest_dir, backtest_additional_log_file)
        self.start_backtest_logging(backtest_additional_log_file)
        self.logger.info("🚀 Starting backtest")
        if end_time is None:
            end_time = dt.datetime.now()
        tz = pytz.timezone(config_dict['trading_time_zone'])
        daily_checks = []
        for i, symbol in enumerate(symbol_list):
            self.logger.info(f"🔄 Processing symbol {i}/{len(symbol_list)}: {symbol}")
            exit_method = watchlist_main_settings[symbol]['Exit']
            exit_sl_input = watchlist_main_settings[symbol]['SL']
            exit_tp_input = watchlist_main_settings[symbol]['TP']
            mode = watchlist_main_settings[symbol].get('Mode', '').lower()
            start_time = self.data_fetcher.get_start_time(end_time, duration_value, duration_unit)
            if start_time.tzinfo is None:
                start_time = tz.localize(start_time)
            if end_time.tzinfo is None:
                end_time = tz.localize(end_time)
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

                    if not is_time_in_trading_windows(current_time.time(), self.config_dict['trading_windows']):
                        self.logger.info(f"⏰ current_time {current_time} not in trading windows {self.config_dict['trading_windows']} 🚫")
                        continue

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
                            units=int(self.config_dict.get("trading_units", 5)),
                            price=last_price,
                            vix=vix,
                            vix_threshold=float(self.config_dict.get("vix_threshold", 20)),
                            vix_reduction_factor=float(self.config_dict.get("vix_reduction_factor", 1)),
                            skip_on_high_vix=bool(self.config_dict.get("skip_on_high_vix", False)),
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

                    # --- Full exit logic using your existing risk management and VWAP ---

                    if symbol in self.positions and self.positions[symbol]['qty'] > 0:
                        current_price = float(df_LTF_slice['close'].iloc[-1])
                        entry_price = self.positions[symbol]['entry_price']
                        entry_time = self.positions[symbol]['entry_time']
                        side = self.positions[symbol]['side'].upper()
                        record = {
                            "qty": self.positions[symbol]['qty'],
                            "profit_taken": self.positions[symbol].get("profit_taken", False)
                        }

                        if exit_method == "E4":
                            vwap_series = calculate_vwap(df_LTF_slice)
                            vwap = vwap_series.iloc[-1] if not vwap_series.empty else None
                        else:
                            vwap = None

                        if exit_method == "E3":
                            res = compute_dynamic_sl_swing(entry_price, current_price, record)
                            sl_price = res['stop_loss']
                            if sl_price > self.positions[symbol]['sl_price']:
                                self.positions[symbol]['sl_price'] = sl_price
                                self.logger.info(f"Moved SL to breakeven for {symbol} at {current_time}")

                            if res['sl_triggered']:
                                self.exit_trade(symbol, current_price, current_time)
                                continue

                            if res['take_70_profit'] and not record.get("profit_taken", False):
                                qty_to_exit = int(record["qty"] * 0.7)
                                if qty_to_exit > 0:
                                    self.exit_trade_partial(symbol, current_price, current_time, qty_to_exit)
                                    self.positions[symbol]["profit_taken"] = True
                                    self.logger.info(f"Took 70% profit partial exit for {symbol} at {current_time}")

                            if res['exit_remaining']:
                                if self.positions[symbol]['qty'] > 0:
                                    self.exit_trade(symbol, current_price, current_time)
                                    continue

                        elif exit_method == "E4":
                            res = compute_hedge_exit_trade(entry_price, current_price, vwap, entry_time, current_time)
                            sl_price = res['stop_loss']
                            if sl_price > self.positions[symbol]['sl_price']:
                                self.positions[symbol]['sl_price'] = sl_price
                                self.logger.info(f"Updated hedge SL for {symbol} at {current_time}")

                            if res['sl_triggered']:
                                self.exit_trade(symbol, current_price, current_time)
                                continue

                            if res['take_profit']:
                                self.exit_trade(symbol, current_price, current_time)
                                continue

                            if res['auto_close']:
                                self.exit_trade(symbol, current_price, current_time)
                                continue

                        elif mode == 'scalping':
                            if current_time.time() >= dt.time(15, 0):
                                self.exit_trade(symbol, current_price, current_time)
                                continue

                        else:
                            sl_price = self.positions[symbol]['sl_price']
                            tp_price = self.positions[symbol]['tp_price']
                            if side == "BUY":
                                if current_price <= sl_price or current_price >= tp_price:
                                    self.exit_trade(symbol, current_price, current_time)
                            elif side == "SELL":
                                if current_price >= sl_price or current_price <= tp_price:
                                    self.exit_trade(symbol, current_price, current_time)

            self.logger.info(f"Backtest complete for {symbol}")

        # After all symbols processed:
        now = dt.datetime.now()
        timestamp_str = time_to_str(now, only_date=True)
        start_str = time_to_str(start_time, only_date=True)
        end_str = time_to_str(end_time, only_date=True)
        daily_checks_file = f"daily_pre_checks_report_{timestamp_str}_{start_str}_{end_str}"
        self.write_daily_checks_to_file(daily_checks, daily_checks_file)
        self.report_results()
        backtest_file = f"{symbol}_{timestamp_str}_{start_str}_{end_str}"
        self.write_trades_to_file(backtest_dir, f"{backtest_file}_backtest_reports")
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
        self.logger.info(f"💾 Daily checks report saved to {filepath}")

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
        self.logger.info(f"💾 Backtest trades report saved to {filepath}")
