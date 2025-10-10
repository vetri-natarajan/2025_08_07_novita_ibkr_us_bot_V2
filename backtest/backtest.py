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

from strategies.strategy import check_HTF_conditions, check_MTF_conditions, check_LTF_conditions, check_TA_confluence
from execution.position_sizer import compute_qty as external_compute_qty
from indicators.atr import calculate_atr

from copy import deepcopy
import sys



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
    def __init__(self, ib, ALWAYS_TFS, watchlist, account_value, data_fetcher, config_dict, premarket, watchlist_main_settings, logger, commission=0.0005):
        self.ib = ib
        self.ALWAYS_TFS = ALWAYS_TFS
        self.watchlist = watchlist
        self.account_value = float(account_value)        
        self.positions = {}
        self.trades = []
        self.data_fetcher = data_fetcher
        self.config_dict = config_dict
        self.commission = self.config_dict["backtest_commission_slippage_percent"]/100
        self.intraday_scalping_exit_time = config_dict["intraday_scalping_exit_time"]
        self.order_testing = self.config_dict['order_testing']
        self.percent_of_account_value = float(self.config_dict['percent_of_account_value'])  # e.g., 0.1 for 10%
        self.premarket = premarket
        self.watchlist_main_settings = watchlist_main_settings
        self.logger = logger
        self.backtest_file_handler = None
        self.total_trades = 0
        self.net_pnl = 0
        self.floating_equity = 0

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
        alloc = max(self.floating_equity * self.percent_of_account_value, 0.0)
        if price <= 0:
            return 0
        qty = int(alloc // price)
        return max(qty, 0)

    def enter_trade(self, symbol_combined, symbol, price, qty, sl_price, tp_price, signal, entry_time, side="BUY"):
        if symbol_combined in self.positions and self.positions[symbol_combined]['qty'] != 0:
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
        self.logger.info(f"Entered trade {symbol_combined} entry_time {entry_time} qty {qty} entry {price:.2f} SL {sl_price:.2f} TP {tp_price:.2f} | entry_comm: {entry_commission:.2f} | equity: {self.account_value:.2f}")

    def exit_trade(self, symbol_combined, symbol, price, time):
        if symbol_combined not in self.positions or self.positions[symbol_combined]['qty'] == 0:
            return

        pos = self.positions[symbol_combined]
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
        self.logger.info(f'🚪 Exit Trade – Proceeds: {proceeds}, Price: {price}, Qty: {qty}, Commission: {exit_commission}')
        pos['exit_price'] = price
        pos['exit_time'] = time
        pos['pnl'] = net_pnl
        pos['qty'] = 0

        for trade in self.trades:
            if trade['symbol_combined'] == symbol_combined and trade['pnl'] is None:
                trade['exit_price'] = price
                trade['exit_time'] = time
                trade['pnl'] = net_pnl
                trade['exit_commission'] = exit_commission
                trade['account_value'] = self.account_value  # snapshot after exit credit
                break

        self.logger.info(f"Exited trade {symbol_combined} qty {qty} at {price:.2f} time {time} gross: {gross_pnl:.2f} exit_comm: {exit_commission:.2f} net: {net_pnl:.2f} | equity: {self.account_value:.2f}")

    def exit_trade_partial(self, symbol_combined, symbol, price, time, qty_to_exit):
        if symbol_combined not in self.positions or self.positions[symbol_combined]['qty'] == 0:
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
        self.logger.info(f"Partially exited trade {symbol_combined} qty {qty_to_exit} at {price:.2f} time {time} net: {net_pnl:.2f} | equity: {self.account_value:.2f}")

 
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
        input_directory = config_dict.get("inputs_directory", "")
        backtest_dir = config_dict['backtest_directory']
        ensure_directory(backtest_dir, self.logger)
    
        now = dt.datetime.now()
        timestamp_str = time_to_str(now, only_date=True)
        addl_log = make_path(backtest_dir, f"backtest_additional_{timestamp_str}.log")
        self.start_backtest_logging(addl_log)
    
        self.logger.info("🚀 Starting backtest")
    
        if end_time is None:
            end_time = dt.datetime.now()
        tz = pytz.timezone(config_dict['trading_time_zone'])
    
        self.account_value = float(config_dict['trading_capital'])
        self.starting_equity = float(self.account_value)
    
        # 1) Preload all data & settings for all symbols once
        universe = {}
        data_cache = {}
        global_time_index = None  # LTF-aligned global market clock
        start_time = self.data_fetcher.get_start_time(end_time, duration_value, duration_unit)
        if start_time.tzinfo is None:
            start_time = tz.localize(start_time)
        if end_time.tzinfo is None:
            end_time = tz.localize(end_time)
    
        for i, symbol_combined in enumerate(symbol_list):
            symbol, _mode = symbol_combined.split('_')
            self.logger.info(f"📌 symbol: {symbol}  ⚙️ mode: {_mode}")
            ta_settings, max_look_back = read_ta_settings(
                symbol_combined, symbol, _mode, input_directory, self.watchlist_main_settings, self.logger
            )
    
            exit_method = watchlist_main_settings[symbol_combined]['Exit']
            exit_sl_input = watchlist_main_settings[symbol_combined]['SL']
            exit_tp_input = watchlist_main_settings[symbol_combined]['TP']
            mode = watchlist_main_settings[symbol_combined].get('Mode', '').lower()
            
            
            dfs = await self.data_fetcher.fetch_multiple_timeframes(
                symbol_combined, symbol, self.ALWAYS_TFS,  start_time, end_time, save=save_data, load=load_data
            )
            
            data_cache[symbol_combined] = dfs
            
            timeframes = watchlist_main_settings[symbol_combined]['Parsed TF']
            df_HTF = dfs.get(timeframes[0])
            df_MTF = dfs.get(timeframes[1])
            df_LTF = dfs.get(timeframes[2])
            
            '''
            self.logger.info(f"📝 head HTF===> \n{df_HTF.head()} ")
            self.logger.info(f"📝 tail HTF===> \n{df_HTF.tail()} ")
            self.logger.info(f"📝 head MTF===> \n{df_MTF.head()} ")
            self.logger.info(f"📝 tail MTF===> \n{df_MTF.tail()} ")
            self.logger.info(f"📝 head LTF===> \n{df_LTF.head()} ")
            self.logger.info(f"📝 tail LTF===> \n{df_LTF.tail()} ")
            '''
            
            skip_HTF = watchlist_main_settings[symbol_combined]['Skip HTF'].strip().upper() in ['Y', 'YES', 'TRUE', '1' ]
            skip_MTF = watchlist_main_settings[symbol_combined]['Skip MTF'].strip().upper() in ['Y', 'YES', 'TRUE', '1' ]
            skip_LTF = watchlist_main_settings[symbol_combined]['Skip LTF'].strip().upper() in ['Y', 'YES', 'TRUE', '1' ]            
            
    
            if (df_LTF is None or df_LTF.empty) and not skip_LTF:
                self.logger.warning(f"⚠️ Skipping LTF {symbol_combined} due to missing data.")
                continue

            if (df_MTF is None or df_MTF.empty) and not skip_MTF:
                self.logger.warning(f"⚠️ Skipping MTF {symbol_combined} due to missing data.")
                continue
            
            if (df_HTF is None or df_HTF.empty) and not skip_HTF:
                self.logger.warning(f"⚠️ Skipping HTF {symbol_combined} due to missing data.")
                continue
            
            # Normalize indexes to UTC and ensure monotonicity
            #df_HTF.index = ensure_utc(pd.to_datetime(df_HTF.index)).sort_values()
            #df_MTF.index = ensure_utc(pd.to_datetime(df_MTF.index)).sort_values()
            #df_LTF.index = ensure_utc(pd.to_datetime(df_LTF.index)).sort_values()
    
            # Collect for universe
            universe[symbol_combined] = dict(
                symbol=symbol,
                ta_settings=ta_settings,
                max_look_back=max_look_back,
                exit_method=exit_method,
                exit_sl_input=exit_sl_input,
                exit_tp_input=exit_tp_input,
                mode=mode,
                timeframes=timeframes, 
                df_HTF=df_HTF,
                df_MTF=df_MTF,
                df_LTF=df_LTF,
                skip_HTF=skip_HTF, 
                skip_MTF=skip_MTF, 
                skip_LTF=skip_LTF
                
            )
    
            # Build/expand global LTF clock
            global_time_index = df_LTF.index if global_time_index is None else global_time_index.union(df_LTF.index)
    
        if not universe or global_time_index is None or len(global_time_index) == 0:
            self.logger.warning("No data loaded for any symbols; aborting.")
            self.stop_backtest_logging()
            return
    
        global_time_index = global_time_index.sort_values()
    
        # 2) Pre-compute unique trading days for premarket checks from global clock (localize to trading TZ for day boundaries)
        global_days_local = pd.DatetimeIndex(global_time_index.tz_convert(tz)).normalize().unique()
        self.logger.info(f"📅 Backtesting {len(universe)} symbols over {len(global_days_local)} trading days total")
    
        daily_checks = []
        # 3) Outer loop: time first (event/heartbeat)
        #   We will do day-level premarket once per day, then iterate all bars in that day’s window.
        #   Within each bar, iterate symbols.
        
        for current_day_local in tqdm(global_days_local, desc="Days", position=0, leave=True):
            day_passed, reason = self.premarket.run_checks_for_day(current_day_local.date())
            if not any(d == current_day_local.date() for (d, _, _) in daily_checks):
                daily_checks.append((current_day_local.date(), day_passed, reason))
    
            if not day_passed:
                self.logger.warning(f"⚠️ Pre-market failed on {current_day_local.date()}: {reason}")
                continue
    
            # Day window (local day to UTC bounds)
            day_start_local = current_day_local
            day_end_local = current_day_local + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
            day_mask = (global_time_index.tz_convert(tz) >= day_start_local) & (global_time_index.tz_convert(tz) <= day_end_local)
            day_times = global_time_index[day_mask]
    
            for current_time_utc in tqdm(day_times, desc=f"Bars {current_day_local.date()}", position=1, leave=False):
                # Respect trading windows in local time
                current_time_local = current_time_utc.tz_convert(tz)
                if not is_time_in_trading_windows(current_time_local.time(), self.config_dict['trading_windows']):
                    continue
    
                # 4) For each symbol, compute slices up to t and run logic
                for symbol_combined, meta in universe.items():
                    
                    in_trade = symbol_combined in self.positions and self.positions[symbol_combined]['qty'] > 0
                    if not in_trade:
                        symbol = meta['symbol']
                        df_LTF = meta['df_LTF']
                        
                        skip_HTF = meta['skip_HTF']
                        skip_MTF = meta['skip_MTF']
                        skip_LTF = meta['skip_LTF']
                        
                        timeframes = meta['timeframes']
                    
                        
                        # Skip if this symbol has no bar up to current_time_utc yet
                        if current_time_utc < df_LTF.index[0]:
                            continue
        
                        df_LTF_slice = df_LTF.loc[:current_time_local]
                        if df_LTF_slice.empty:
                            continue
        
                        # HTF/MTF aligned up to t
                        df_HTF_slice = meta['df_HTF'].loc[:current_time_local]
                        df_MTF_slice = meta['df_MTF'].loc[:current_time_local]                     

                        '''
                        self.logger.info(f"📝 head HTF slice===> \n{df_HTF_slice.head(5)} ")
                        self.logger.info(f"📝 tail HTF slice===> \n{df_HTF_slice.tail(5)} ")
                        self.logger.info(f"📝 head MTF slice===> \n{df_MTF_slice.head(5)} ")
                        self.logger.info(f"📝 tail MTF slice ===> \n{df_MTF_slice.tail(5)} ")     
                        self.logger.info(f"📝 head LTF slice===> \n{df_LTF_slice.head(5)} ")
                        '''              
                        
                        self.logger.info(f"📝 tail LTF slice===> \n{df_LTF_slice[['open', 'high', 'low', 'close']].tail(5)} ")                    
                        
                        if df_HTF_slice.empty or df_MTF_slice.empty:
                            continue
                        data_cache_slice = deepcopy(data_cache)

                        #data cache/slices for all technical indicators...above are only for HTF, MTF and LTF timeframes
                        for symbol_temp, df_dict in data_cache_slice.items():
                            
                            #self.logger.info(f"symbol_combined data_cache ===> {symbol_combined}")
                            #self.logger.info(f"df_dict data_cache===> {df_dict}")
                            for tf, df in df_dict.items():
                                #self.logger.info(f"tf==> {tf}")
                                if tf == '1 min':
                                    #self.logger.info(f"📝 df dict before===> {df_dict[tf].tail()}")
                                    pass
                                    
                                df_dict[tf] = df.loc[:current_time_local]
                                
                                if tf == '1 min':
                                    #self.logger.info(f"📝 df after===> {df_dict[tf].tail()}")
                                    pass
                            
                            #self.logger.info(f"symbol_combined data_cache after===> {symbol_combined}")
                            #self.logger.info(f"df_dict data_cache after ===> {df_dict}")
                        
                        #testing indicators
                        #check_TA_confluence(symbol_combined, self.ALWAYS_TFS, data_cache_slice, ta_settings, watchlist_main_settings, self.logger)
                        #continue
                        
                        # HTF/MTF gating unless in order_testing
                        if not self.order_testing:
                            if not skip_HTF:
                                okHTF = check_HTF_conditions(symbol_combined, symbol, watchlist_main_settings, meta['ta_settings'], meta['max_look_back'], df_HTF_slice, self.logger)
                            else: 
                                okHTF = True
                                self.logger.info(f"⏭️🕰️ Skip HTF enabled — skipping {timeframes[0]} checks for [{symbol_combined}] ✅")
                            if not skip_MTF: 
                                okMTF = check_MTF_conditions(symbol_combined, symbol, watchlist_main_settings, meta['ta_settings'], meta['max_look_back'], df_MTF_slice, self.logger)
                            else: 
                                okMTF = True
                                self.logger.info(f"⏭️🕰️ Skip MTF enabled — skipping {timeframes[1]} checks for [{symbol_combined}] ✅")          
                                
                            if not skip_HTF and not okHTF:
                                continue

                            if not skip_MTF and not okMTF:
                                continue

                        if not skip_LTF:
                            okLTF = check_LTF_conditions(symbol_combined, symbol, watchlist_main_settings, meta['ta_settings'], meta['max_look_back'], df_LTF_slice, df_HTF_slice, self.logger)
                        
                        else: 
                            okLTF = True
                            self.logger.info(f"⏭️🕰️ Skip LTF enabled — skipping {timeframes[2]} checks for [{symbol_combined}] ✅")          
                            
                        if not skip_LTF and not okLTF:
                            self.logger.info("⚠️ LTF cheks not okay... continuing")
                            continue
                        
                        if mode == 'scalping': # in scalping mode don't enter after the exit time
                            exit_hour, exit_min = self.intraday_scalping_exit_time.split(':')
                            exit_time = dt.time(int(exit_hour), int(exit_min))  
                            self.logger.info(f"⏱️ Trade time check — Now: {current_time_local.time()} | Exit scheduled: {exit_time}")
                            if current_time_local.time() >= exit_time:
                                continue
                            
                        sig = check_TA_confluence(symbol_combined, self.ALWAYS_TFS, data_cache_slice, ta_settings, watchlist_main_settings, self.logger)

                        
                        # Entry
                        if sig and (symbol_combined not in self.positions or self.positions[symbol_combined]['qty'] == 0):
                            
                            last_price = float(df_LTF_slice['close'].iloc[-1])
        
                            #qty_equity = self.compute_qty(last_price)
                            vix = self.premarket.get_close_price(self.premarket.vix_df, current_time_local.date())
                            qty = external_compute_qty(
                                self.account_value,
                                self.percent_of_account_value,
                                units=int(self.config_dict.get("trading_units", 5)),
                                price=last_price,
                                vix=vix,
                                logger = self.logger,
                                vix_threshold=float(self.config_dict.get("vix_threshold", 20)),
                                vix_reduction_factor=float(self.config_dict.get("vix_reduction_factor", 1)),
                                skip_on_high_vix=bool(self.config_dict.get("skip_on_high_vix", False)),
                            )
                            #qty = max(min(qty_equity, qty_vix), 0)
                            if qty <= 0:
                                continue
        
                            exit_method = meta['exit_method']
                            sl_in = meta['exit_sl_input']
                            tp_in = meta['exit_tp_input']
        
                            # Compute SL/TP
                            last_price = float(df_LTF_slice['close'].iloc[-1])
                            if exit_method == "E1":
                                sl_price, tp_price = compute_fixed_sl_tp(last_price, sl_pct=sl_in, tp_pct=tp_in)
                                self.logger.info(f"🚀 E1 Fixed Stop Calculated | SL: {sl_price}, TP: {tp_price}")
                            elif exit_method == "E2":
                                try:
                                    atr_val = float(calculate_atr(df_LTF_slice, 5).iloc[-1])
                                except Exception:
                                    atr_val = None
                                if atr_val is not None:
                                    sl_price, tp_price = compute_atr_sl_tp(last_price, atr_val, k_sl=sl_in, k_tp=tp_in)
                                    self.logger.info(f"🚀 E2 ATR Stop Calculated | ATR: {atr_val}, SL: {sl_price}, TP: {tp_price}")

                                else:
                                    sl_price, tp_price = compute_fixed_sl_tp(last_price, sl_pct=sl_in, tp_pct=tp_in)
                                    self.logger.info(f"🚀 Default E1 Fixed Stop Calculated | SL: {sl_price}, TP: {tp_price}")
                            elif exit_method in ['E3', 'E4']:
                                sl_price, tp_price = compute_fixed_sl_tp(last_price, sl_pct=2, tp_pct=4)
                                self.logger.info(f"🚀 Initial E1 Fixed Stop Calculated for E3/E4 | SL: {sl_price}, TP: {tp_price}")
                            else:
                                sl_price, tp_price = compute_fixed_sl_tp(last_price, sl_pct=sl_in, tp_pct=tp_in)
        
                            self.enter_trade(
                                symbol_combined, symbol, last_price, qty, sl_price, tp_price, sig,
                                entry_time=current_time_utc, side="BUY"
                            )                              
                    else: 

                        df_LTF_slice = df_LTF.loc[:current_time_local]
                        self.logger.info(
                                        f"📝 Tail LTF slice (H, L, C) ===> \n{df_LTF_slice[['open', 'high', 'low', 'close']].tail(5)}"
                                        )
                        pass
                    
    
                    # Exit management
                    if symbol_combined in self.positions and self.positions[symbol_combined]['qty'] > 0:
                        
                        current_price = float(df_LTF_slice['close'].iloc[-1])
                        current_low = float(df_LTF_slice['low'].iloc[-1])
                        current_high = float(df_LTF_slice['high'].iloc[-1])
                        entry_price = self.positions[symbol_combined]['entry_price']
                        entry_time = self.positions[symbol_combined]['entry_time']
                        side = self.positions[symbol_combined]['side'].upper()
                        record = {
                            "qty": self.positions[symbol_combined]['qty'],
                            "profit_taken": self.positions[symbol_combined].get("profit_taken", False)
                        }
                        exit_method = meta['exit_method']
                        mode = meta['mode']
                        self.logger.info(
                            f"📌 Already in trade... ⏰ {current_time_utc} ===> [{symbol_combined}] 💰 Entry: {entry_price} | 🕒 Time: {entry_time} | 🚪 Exit method: {exit_method}"
                        )


                        if exit_method == "E3":
                            res = compute_dynamic_sl_swing(entry_price, current_price, record)
                            sl_price = res['stop_loss']
                            if sl_price > self.positions[symbol_combined]['sl_price']:
                                self.positions[symbol_combined]['sl_price'] = sl_price
                            if current_low <= sl_price:
                                self.logger.info(f"🛑 SL triggered — Stoploss {sl_price}")
                                self.exit_trade(symbol_combined, symbol, sl_price, current_time_utc)
                                continue
                            if current_high >= res["target_70"] and not record.get("profit_taken", False):
                                qty_to_exit = int(record["qty"] * 0.7)
                                if qty_to_exit > 0:
                                    self.logger.info(f"🎯 Partial Exit — 9% First target reached | Target: {res['target_70']}")
                                    self.exit_trade_partial(symbol_combined, symbol, res["target_70"], current_time_utc, qty_to_exit)
                                    self.positions[symbol_combined]["profit_taken"] = True
                            if current_high >= res["target_30"]:
                                if self.positions[symbol_combined]['qty'] > 0:
                                    self.logger.info(f"🎯 Partial Exit — 12% Second target reached | Target: {res['target_30']}")
                                    self.exit_trade(symbol_combined, symbol, res["target_30"], current_time_utc)
                                    continue
    
                        if exit_method == "E4":
                            vwap_series = calculate_vwap(df_LTF_slice)
                            vwap = vwap_series.iloc[-1] if not vwap_series.empty else None
                            res = compute_hedge_exit_trade(entry_price, current_price, vwap, entry_time, current_time_utc)
                            
                            sl_price = res['stop_loss']
                            tp_price = res['tp_price']
                            
                            self.logger.info(f"🧮 Calculating dynamic stop loss — 📊 VWAP: {vwap} | 🛑 SL: {sl_price} | 🎯 TP: {tp_price}")

                            
                            if sl_price > self.positions[symbol_combined]['sl_price']:
                                self.positions[symbol_combined]['sl_price'] = sl_price
                            
                            sl_reached = current_low <= sl_price
                            tp_reached = current_high >= tp_price
                            auto_close_reached = res['auto_close']
                            
                            if sl_reached or tp_reached or auto_close_reached:
                                exit_price = None
                                if sl_reached:
                                    self.logger.info(f"🛑 SL triggered — Stoploss {sl_price}")
                                    exit_price = sl_price
                                elif tp_reached: 
                                    self.logger.info(f"🎯 Target reached | Target: {tp_price}")
                                    exit_price = tp_price
                                elif auto_close_reached: 
                                    self.logger.info(f"🚨 Max holding period reached — Closing at {current_price}")
                                    exit_price = current_price
                                    
                                self.exit_trade(symbol_combined, symbol, exit_price, current_time_utc)
                                continue
    
                        if mode == 'scalping':
                            exit_hour, exit_min = self.intraday_scalping_exit_time.split(':')
                            exit_time = dt.time(int(exit_hour), int(exit_min))  
                            self.logger.info(f"⏱️ Trade time check — Now: {current_time_local.time()} | Exit scheduled: {exit_time}")
                            if current_time_local.time() >= exit_time:
                                self.logger.info(
                                                f"⏰ Time-based Exit triggered ===> 📉 Current: {current_price} | 🛑 SL: {sl_price} | 🎯 TP: {tp_price}"
                                                )
                                self.exit_trade(symbol_combined, symbol, current_price, current_time_utc)
                                continue
                            else:
                                self.logger.info("⏱️ Time exit condition not satisfied")                                
    
                        if exit_method in ["E1", "E2"]:  # E1/E2 fixed/ATR
                            sl_price = self.positions[symbol_combined]['sl_price']
                            tp_price = self.positions[symbol_combined]['tp_price']
                            if side == "BUY":
                                exit_price = None
                                if current_low <= sl_price or current_high >= tp_price:
                                    if current_low <= sl_price:
                                        self.logger.info(f"🛑 SL triggered — Stoploss {sl_price}")
                                        exit_price = sl_price
                                    elif current_high >= tp_price: 
                                        self.logger.info(f"🎯 Target reached | Target: {tp_price}")
                                        exit_price = tp_price                          
                                    self.exit_trade(symbol_combined, symbol, exit_price, current_time_utc)
                    else: 
                        #self.logger.info("Not in trade ===> current_time_utc {current_time_utc}")
                        pass
    
        # 5) Finalize
        now = dt.datetime.now()
        timestamp_str = time_to_str(now, only_date=True)
        start_str = time_to_str(start_time, only_date=True)
        end_str = time_to_str(end_time, only_date=True)
    
        self.ending_equity = float(self.account_value)
        self.logger.info(f"Starting equity: {self.starting_equity:.2f} | Ending equity: {self.ending_equity:.2f}")
    
        daily_checks_file = f"daily_pre_checks_report_{timestamp_str}_{start_str}_{end_str}"
        self.write_daily_checks_to_file(daily_checks, daily_checks_file)
    
        self.report_results()
        backtest_file = f"portfolio_{timestamp_str}_{start_str}_{end_str}"
        self.write_trades_to_file(backtest_dir, f"{backtest_file}_backtest_reports")
        self.stop_backtest_logging()
        self.ib.disconnect()

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
