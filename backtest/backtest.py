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
from execution.position_sizer import compute_qty as external_compute_qty
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
    def __init__(self, ib, watchlist, account_value, data_fetcher, config_dict, premarket, watchlist_main_settings, logger, commission=0.0005):
        self.ib = ib
        self.watchlist = watchlist
        self.account_value = float(account_value)
        self.commission = float(commission)
        self.positions = {}
        self.trades = []
        self.data_fetcher = data_fetcher
        self.config_dict = config_dict
        self.order_testing = self.config_dict['order_testing']
        self.percent_of_account_value = float(self.config_dict['percent_of_account_value'])  # e.g., 0.1 for 10%
        self.premarket = premarket
        self.watchlist_main_settings = watchlist_main_settings
        self.logger = logger
        self.backtest_file_handler = None

        # Equity tracking for percent-of-equity sizing
        self.starting_equity = float(account_value)
        self.ending_equity = float(account_value)

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

    def compute_qty(self, price: float) -> int:
        """
        Percent-of-equity sizing:
        alloc = account_value * percent_of_account_value
        qty = floor(alloc / price)
        """
        alloc = max(self.account_value * self.percent_of_account_value, 0.0)
        if price <= 0:
            return 0
        qty = int(alloc // price)
        return max(qty, 0)

    def enter_trade(self, symbol_combined, symbol, price, qty, sl_price, tp_price, signal, entry_time, side="BUY"):
        if symbol in self.positions and self.positions[symbol_combined]['qty'] != 0:
            self.logger.info(f"Already have active position on {symbol}, skipping entry")
            return

        # Entry commission and cash debit
        entry_commission = price * qty * self.commission
        cash_outlay = price * qty + entry_commission

        # If insufficient cash/equity for entry, skip
        if cash_outlay > self.account_value:
            self.logger.info(f"Insufficient equity for entry {symbol}: need {cash_outlay:.2f}, have {self.account_value:.2f}")
            return

        # Debit cash/equity at entry
        self.account_value -= cash_outlay

        self.positions[symbol_combined] = {
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
            'symbol_combined': symbol_combined,
            'symbol': symbol,
            'qty': qty,
            'entry_price': price,
            'sl_price': sl_price,
            'tp_price': tp_price,
            'signal': signal,
            'side': side,
            'entry_time': entry_time,
            'exit_time': None,
            'exit_price': None,
            'pnl': None,
            'entry_commission': entry_commission,
            'exit_commission': None,
            'account_value': self.account_value  # snapshot after entry debit
        })
        self.logger.info(f"Entered trade {symbol} qty {qty} entry {price:.2f} SL {sl_price:.2f} TP {tp_price:.2f} | entry_comm: {entry_commission:.2f} | equity: {self.account_value:.2f}")

    def exit_trade(self, symbol, price, time):
        if symbol not in self.positions or self.positions[symbol]['qty'] == 0:
            return

        pos = self.positions[symbol]
        qty = pos['qty']
        entry_price = pos['entry_price']
        side = pos['side'].upper()

        # Realized P&L for this exit leg
        gross_pnl = (price - entry_price) * qty if side == "BUY" else (entry_price - price) * qty
        exit_commission = price * qty * self.commission
        # Entry commission already debited at entry; subtract only exit leg here
        net_pnl = gross_pnl - exit_commission
        self.logger.info(f'🚪 In exit – self.account_value: {self.account_value} ')
        # Cash proceeds from exit leg credited back
        proceeds = price * qty - exit_commission
        self.account_value += proceeds
        self.logger.info(f'🚪 Exit Trade – Partial Proceeds: {proceeds}, Price: {price}, Qty: {qty}, Commission: {exit_commission}')
        pos['exit_price'] = price
        pos['exit_time'] = time
        pos['pnl'] = net_pnl
        pos['qty'] = 0

        for trade in self.trades:
            if trade['symbol'] == symbol and trade['pnl'] is None:
                trade['exit_price'] = price
                trade['exit_time'] = time
                trade['pnl'] = net_pnl
                trade['exit_commission'] = exit_commission
                trade['account_value'] = self.account_value  # snapshot after exit credit
                break

        self.logger.info(f"Exited trade {symbol} qty {qty} at {price:.2f} time {time} gross: {gross_pnl:.2f} exit_comm: {exit_commission:.2f} net: {net_pnl:.2f} | equity: {self.account_value:.2f}")

    def exit_trade_partial(self, symbol_combined, symbol, price, time, qty_to_exit):
        if symbol not in self.positions or self.positions[symbol]['qty'] == 0:
            return

        pos = self.positions[symbol_combined]
        if qty_to_exit > pos['qty']:
            qty_to_exit = pos['qty']

        entry_price = pos['entry_price']
        side = pos['side'].upper()
        qty_remaining = pos['qty'] - qty_to_exit

        gross_pnl = (price - entry_price) * qty_to_exit if side == "BUY" else (entry_price - price) * qty_to_exit
        exit_commission = price * qty_to_exit * self.commission
        net_pnl = gross_pnl - exit_commission
        
        self.logger.info(f'🚪 In exit partial – self.account_value: {self.account_value} ')
        # Credit proceeds of the partial exit
        proceeds = price * qty_to_exit - exit_commission
        self.account_value += proceeds
        
        self.logger.info(f'🚪 Exit Trade partial – Partial Proceeds: {proceeds}, Price: {price}, Qty: {qty_to_exit}, Commission: {exit_commission}')


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
            'symbol_combined': symbol_combined,
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
            'pnl': net_pnl,
            'entry_commission': 0.0,   # entry comm already charged on full qty at entry
            'exit_commission': exit_commission,
            'account_value': self.account_value  # snapshot after partial credit
        })
        self.logger.info(f"Partially exited trade {symbol} qty {qty_to_exit} at {price:.2f} time {time} net: {net_pnl:.2f} | equity: {self.account_value:.2f}")

    async def run_backtest(
        self,
        config_dict,
        symbol_list,
        watchlist_main_settings,
        duration_value=2,
        duration_unit='weeks',
        end_time=None,
        save_data=False,
        load_data=False
    ):
        input_directory = config_dict.get("inputs_directory", "")
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

        # Initialize equity once (do NOT reset per symbol)
        self.account_value = float(config_dict['trading_capital'])
        self.starting_equity = float(self.account_value)

        daily_checks = []

        for i, symbol_combined in enumerate(symbol_list):
            symbol, mode = symbol_combined.split('_')
            self.logger.info(f"📌 symbol: {symbol}    ⚙️ mode: {mode}")
            self.logger.info(f"🔄 Processing symbol {i}/{len(symbol_list)}: {symbol}")

            ta_settings, max_look_back = read_ta_settings(symbol_combined, symbol, mode, input_directory, self.watchlist_main_settings, self.logger)

            # Do NOT reset equity per symbol
            exit_method = watchlist_main_settings[symbol_combined]['Exit']
            exit_sl_input = watchlist_main_settings[symbol_combined]['SL']
            exit_tp_input = watchlist_main_settings[symbol_combined]['TP']
            mode = watchlist_main_settings[symbol_combined].get('Mode', '').lower()

            start_time = self.data_fetcher.get_start_time(end_time, duration_value, duration_unit)
            if start_time.tzinfo is None:
                start_time = tz.localize(start_time)
            if end_time.tzinfo is None:
                end_time = tz.localize(end_time)

            dfs, timeframes = await self.data_fetcher.fetch_multiple_timeframes(symbol_combined, symbol, start_time, end_time, save=save_data, load=load_data)
            df_HTF = dfs.get(timeframes[0])
            df_MTF = dfs.get(timeframes[1])
            df_LTF = dfs.get(timeframes[2])

            if df_LTF is None or df_LTF.empty or df_HTF is None or df_MTF is None:
                self.logger.warning(f"⚠️ Skipping {symbol_combined} due to missing data.")
                continue

            unique_days = df_LTF.index.normalize().unique()
            self.logger.info(f"📅 Backtesting {symbol_combined} for {len(unique_days)} trading days")

            for current_day in tqdm(unique_days, desc=f"Days for {symbol_combined}", position=1, leave=False):
                self.logger.info(f'\n📅 Current day ==> {current_day}')
                passed, reason = self.premarket.run_checks_for_day(current_day.date())
                if not any(d == current_day.date() for d in daily_checks):
                    daily_checks.append((current_day.date(), passed, reason))
                if not passed:
                    self.logger.warning(f"⚠️ Pre-market check failed for {symbol_combined} on {current_day.date()}: {reason}")
                    continue

                day_start = current_day
                day_end = current_day + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
                df_LTF_day = df_LTF.loc[(df_LTF.index >= day_start) & (df_LTF.index <= day_end)]

                for idx in tqdm(range(len(df_LTF_day)), desc=f"Processing bars {symbol_combined} {current_day.date()}", position=2, leave=False):
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

                    if not self.order_testing:
                        okHTF = check_HTF_conditions(symbol_combined, symbol, watchlist_main_settings, ta_settings, max_look_back, df_HTF_slice, self.logger)
                        okMTF = check_MTF_conditions(symbol_combined, symbol, watchlist_main_settings, ta_settings, max_look_back, df_MTF_slice, self.logger)
                        if not (okHTF and okMTF):
                            continue
                        if len(df_LTF_slice) < 10:
                            continue
                    else:
                        pass

                    sig = check_LTF_conditions(symbol_combined, symbol, watchlist_main_settings, ta_settings, max_look_back, df_LTF_slice, df_HTF_slice, self.logger)

                    # Entry
                    if sig and (symbol_combined not in self.positions or self.positions[symbol_combined]['qty'] == 0):
                        try:
                            self.logger.info(
                                f"df_HTF_slice --> {df_HTF_slice.tail()} | "
                                f"df_MTF_slice --> {df_MTF_slice.tail()} | "
                                f"df_LTF_slice --> {df_LTF_slice.tail()}"
                            )
                        except Exception as e:
                            self.logger.info(f"Exception: {e}")

                        last_price = float(df_LTF_slice['close'].iloc[-1])

                        # Percent-of-equity sizing; optionally still use external vix-aware sizing if desired.
                        # Here, prioritize percent-of-equity. If you want to retain VIX logic, uncomment external call and
                        # then cap by percent-of-equity qty.
                        qty_equity = self.compute_qty(last_price)

                        # If you want to apply VIX-aware reduction as an overlay:
                        vix = self.premarket.get_close_price(self.premarket.vix_df, current_day.date())
                        qty_vix = external_compute_qty(
                            self.account_value,
                            self.percent_of_account_value,
                            units=int(self.config_dict.get("trading_units", 5)),
                            price=last_price,
                            vix=vix,
                            vix_threshold=float(self.config_dict.get("vix_threshold", 20)),
                            vix_reduction_factor=float(self.config_dict.get("vix_reduction_factor", 1)),
                            skip_on_high_vix=bool(self.config_dict.get("skip_on_high_vix", False)),
                        )

                        # Final qty is the minimum (risk overlays should not exceed equity allocation)
                        qty = max(min(qty_equity, qty_vix), 0)

                        if qty <= 0:
                            continue

                        # Compute SL/TP
                        if exit_method == "E1":
                            sl_price, tp_price = compute_fixed_sl_tp(
                                last_price,
                                sl_pct=exit_sl_input,
                                tp_pct=exit_tp_input
                            )
                        elif exit_method == "E2":  # ATR
                            try:
                                atr_val = float(calculate_atr(df_LTF_slice, 5).iloc[-1])
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
                        elif exit_method in ['E3', 'E4']:
                            exit_sl_input_over = 2
                            exit_tp_input_over = 4
                            sl_price, tp_price = compute_fixed_sl_tp(
                                last_price,
                                sl_pct=exit_sl_input_over,
                                tp_pct=exit_tp_input_over
                            )
                        else:
                            sl_price, tp_price = compute_fixed_sl_tp(
                                last_price,
                                sl_pct=exit_sl_input,
                                tp_pct=exit_tp_input
                            )

                        entry_time = current_time
                        self.enter_trade(symbol_combined, symbol, last_price, qty, sl_price, tp_price, sig, entry_time, side="BUY")

                    # Exit management for active positions
                    if symbol_combined in self.positions and self.positions[symbol_combined]['qty'] > 0:
                        current_price = float(df_LTF_slice['close'].iloc[-1])
                        entry_price = self.positions[symbol_combined]['entry_price']
                        entry_time = self.positions[symbol_combined]['entry_time']
                        side = self.positions[symbol_combined]['side'].upper()
                        record = {
                            "qty": self.positions[symbol_combined]['qty'],
                            "profit_taken": self.positions[symbol_combined].get("profit_taken", False)
                        }

                        if exit_method == "E3":
                            res = compute_dynamic_sl_swing(entry_price, current_price, record)
                            sl_price = res['stop_loss']
                            if sl_price > self.positions[symbol_combined]['sl_price']:
                                self.positions[symbol_combined]['sl_price'] = sl_price
                                self.logger.info(f"🟡 SL moved up for {symbol_combined} at {current_time}")
                            if res['sl_triggered']:
                                self.logger.info(f"🔴 SL triggered for {symbol_combined} at {current_time}! Exiting.")
                                self.exit_trade(symbol_combined, symbol, current_price, current_time)
                                continue
                            if res['take_70_profit'] and not record.get("profit_taken", False):
                                qty_to_exit = int(record["qty"] * 0.7)
                                if qty_to_exit > 0:
                                    self.exit_trade_partial(symbol_combined, symbol, current_price, current_time, qty_to_exit)
                                    self.positions[symbol_combined]["profit_taken"] = True
                                    self.logger.info(f"💰 Partial exit (70%) for {symbol_combined} at {current_time}")
                            if res['exit_remaining']:
                                if self.positions[symbol_combined]['qty'] > 0:
                                    self.exit_trade(symbol_combined, current_price, current_time)
                                    self.logger.info(f"🔵 Final exit for {symbol_combined} at {current_time}")
                                    continue

                        elif exit_method == "E4":
                            vwap_series = calculate_vwap(df_LTF_slice)
                            vwap = vwap_series.iloc[-1] if not vwap_series.empty else None
                            res = compute_hedge_exit_trade(entry_price, current_price, vwap, entry_time, current_time)
                            sl_price = res['stop_loss']
                            if sl_price > self.positions[symbol_combined]['sl_price']:
                                self.positions[symbol_combined]['sl_price'] = sl_price
                                self.logger.info(f"🟠 Hedge SL moved up for {symbol_combined} at {current_time}")
                            if res['sl_triggered']:
                                self.logger.info(f"🛑 Hedge SL triggered for {symbol_combined} at {current_time}, exiting.")
                                self.exit_trade(symbol_combined, symbol, current_price, current_time)
                                continue
                            if res['take_profit']:
                                self.logger.info(f"✅ Take profit hit for {symbol_combined} at {current_time}, exiting.")
                                self.exit_trade(symbol_combined, symbol, current_price, current_time)
                                continue
                            if res['auto_close']:
                                self.logger.info(f"🚪 Auto-close for {symbol} at {current_time}.")
                                self.exit_trade(symbol_combined, symbol, current_price, current_time)
                                continue

                        elif mode == 'scalping':
                            if current_time.time() >= dt.time(15, 0):
                                self.exit_trade(symbol_combined, symbol, current_price, current_time)
                                self.logger.info(f"🔔 Scalping exit for {symbol_combined} at {current_time}")
                                continue

                        else: #E1, E2 exit
                            sl_price = self.positions[symbol_combined]['sl_price']
                            tp_price = self.positions[symbol_combined]['tp_price']
                            if side == "BUY":
                                if current_price <= sl_price:
                                    self.logger.info(f"🔴 SL hit for {symbol_combined} at {current_time}! Exiting.")
                                    self.exit_trade(symbol_combined, symbol, current_price, current_time)
                                elif current_price >= tp_price:
                                    self.logger.info(f"✅ TP hit for {symbol_combined} at {current_time}! Exiting.")
                                    self.exit_trade(symbol_combined, symbol, current_price, current_time)
                            elif side == "SELL":
                                if current_price >= sl_price:
                                    self.logger.info(f"🔴 SL hit for {symbol_combined} at {current_time}! Exiting.")
                                    self.exit_trade(symbol_combined, symbol, current_price, current_time)
                                elif current_price <= tp_price:
                                    self.logger.info(f"✅ TP hit for {symbol_combined} at {current_time}! Exiting.")
                                    self.exit_trade(symbol_combined, symbol, current_price, current_time)

            self.logger.info(f"Backtest complete for {symbol_combined}")

        # Finalize equity and reports
        now = dt.datetime.now()
        timestamp_str = time_to_str(now, only_date=True)
        start_str = time_to_str(start_time, only_date=True)
        end_str = time_to_str(end_time, only_date=True)

        # Ending equity after all trades
        self.ending_equity = float(self.account_value)
        self.logger.info(f"Starting equity: {self.starting_equity:.2f} | Ending equity: {self.ending_equity:.2f}")

        daily_checks_file = f"daily_pre_checks_report_{timestamp_str}_{start_str}_{end_str}"
        self.write_daily_checks_to_file(daily_checks, daily_checks_file)

        self.report_results()

        # Use last symbol label in file name; optionally aggregate name across symbols
        backtest_file = f"portfolio_{timestamp_str}_{start_str}_{end_str}"
        self.write_trades_to_file(backtest_dir, f"{backtest_file}_backtest_reports")

        self.stop_backtest_logging()
        self.ib.disconnect()




    async def run_backtest_new(
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
        
        pass

    def report_results(self):
        total_trades = len([t for t in self.trades if t.get('pnl') is not None])
        winning_trades = [t for t in self.trades if t.get('pnl') is not None and t['pnl'] > 0]
        losing_trades = [t for t in self.trades if t.get('pnl') is not None and t['pnl'] <= 0]
        total_pnl = sum(t['pnl'] for t in self.trades if t['pnl'] is not None)
        win_rate = len(winning_trades) / total_trades if total_trades > 0 else 0.0

        '''
        total_trades = len([t for t in self.trades])
        winning_trades = [t for t in self.trades if t.get('pnl') and t['pnl'] > 0]
        losing_trades = [t for t in self.trades if t.get('pnl') and t['pnl'] <= 0 ]
        total_pnl = sum(t['pnl'] for t in self.trades if t['pnl'] is not None)
        win_rate = len(winning_trades) / total_trades if total_trades > 0 else 0.0
        '''

        
        self.logger.info("Backtest summary:")
        self.logger.info(f"Starting equity: {self.starting_equity:.2f}")
        self.logger.info(f"Ending equity: {self.ending_equity:.2f}")
        self.logger.info(f"Total trades (legs with realized P&L): {total_trades}")
        self.logger.info(f"Winning trades: {len(winning_trades)}")
        self.logger.info(f"Losing trades: {len(losing_trades)}")
        self.logger.info(f"Win rate: {win_rate*100:.2f}%")
        self.logger.info(f"Net P&L: {total_pnl:.2f}")

        print("Backtest summary:")
        print(f"Starting equity: {self.starting_equity:.2f}")
        print(f"Ending equity: {self.ending_equity:.2f}")
        print(f"Total trades (legs with realized P&L): {total_trades}")
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

        # Optional: prepend a summary header row for starting/ending equity
        summary_header_path = make_path(backtest_dir, f'{file}_summary.txt')
        try:
            with open(summary_header_path, 'w') as sf:
                sf.write(f"Starting equity: {self.starting_equity:.2f}\n")
                sf.write(f"Ending equity: {self.ending_equity:.2f}\n")
        except Exception:
            pass

        for trade in self.trades:
            if trade.get('pnl') is None:
                continue
            cum_pnl += trade['pnl']
            account_value = trade.get('account_value', self.account_value)
            symbol = trade['symbol_combined']
            qty = trade['qty']
            side = 'long' if trade.get('side', 'BUY').upper() == 'BUY' else 'short'
            entry_dt = trade['entry_time']
            exit_dt = trade['exit_time'] or ''
            entry_date = entry_dt.strftime('%Y-%m-%d') if isinstance(entry_dt, (dt.datetime, pd.Timestamp)) else ''
            entry_time = entry_dt.strftime('%H:%M:%S') if isinstance(entry_dt, (dt.datetime, pd.Timestamp)) else ''
            exit_date = exit_dt.strftime('%Y-%m-%d') if isinstance(exit_dt, (dt.datetime, pd.Timestamp)) else ''
            exit_time = exit_dt.strftime('%H:%M:%S') if isinstance(exit_dt, (dt.datetime, pd.Timestamp)) else ''

            row = [
                symbol,
                qty,
                side,
                entry_date,
                entry_time,
                f"{trade['entry_price']:.4f}",
                exit_date,
                exit_time,
                f"{trade['exit_price']:.4f}" if trade['exit_price'] is not None else '',
                f"{trade['pnl']:.4f}",
                f"{cum_pnl:.4f}",
                f"{trade.get('entry_commission', 0.0):.4f}",
                f"{trade.get('exit_commission', 0.0):.4f}",
                account_value
            ]
            rows.append(row)

        with open(filepath, mode='w', newline='') as f:
            writer = csv.writer(f)
            header = [
                'symbol',
                'qty',
                'long/short',
                'entry_date',
                'entry_time',
                'entry_price',
                'exit_date',
                'exit_time',
                'exit_price',
                'profit/loss',
                'cum_profit_loss',
                'entry_commission',
                'exit_commission',
                'account_value'  # snapshot after the leg that produced a realized P&L
            ]
            writer.writerow(header)
            writer.writerows(rows)

        self.logger.info(f"💾 Backtest trades report saved to {filepath}")
