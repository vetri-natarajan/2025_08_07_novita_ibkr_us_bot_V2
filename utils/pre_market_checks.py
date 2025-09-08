# -*- coding: utf-8 -*-
"""
utils/pre_market_checks.py

Purpose: 
    - run the following safety and premarket checks
        1. checks for config file errros
        2. checks for previous losses
        3. ablet o get vi
        2. check for whehter the bot is operating within trading window 
       
        4. checks for vix S&P volatility envelope
"""
from typing import Tuple
import datetime as dt

from data import vix_spx 
from utils.config_validator import validate_config
from utils.time_utils import in_trading_window
from utils.loss_tracker import loss_tracker_class
#from main import logger
from utils.is_in_trading_window import is_time_in_trading_windows


class pre_market_checks:
    def __init__(self, ib, config_dict: dict, loss_tracker:loss_tracker_class,  vix_symbol, spx_symbol, ib_connector, logger):
        self.ib = ib
        self.config_dict = config_dict
        self.loss_tracker_class = loss_tracker
        self.exchange = config_dict["exchange"]
        self.exchange_index = config_dict["exchange_index"]
        self.currency = config_dict["currency"]
        self.trading_windows = config_dict["trading_windows"]
        self.test_run = config_dict["test_run"]
        #print("trading_windows ===>", self.trading_windows)
        self.vix_symbol = vix_symbol
        self.spx_symbol = spx_symbol
        self.ib_connector = ib_connector
        self.logger = logger
        
    async def run_checks(self) -> Tuple[bool, str]:
        self.logger.info("🚦 Running pre-market checks 🕒...")
        #1. config validation
        self.logger.info("🔍 Checking config input validity...")
        ok, msg = validate_config(self.config_dict)
        if not ok:
            return False, f"Config validation failed{msg}"
        
        
        #2. loss tracker
        self.logger.info("🔍 Checking whether trading is halted ⛔️⏸️...")
        if self.loss_tracker_class.is_halted():
            untill = self.loss_tracker_class.halted_until()
            return False, f"Trading halted due to consecutive lsses until{untill}"
        #3. vix related
        self.logger.info("📊 Getting VIX and SPY data for Rule of 16 analysis... ⚡️📈")
        try: 
            await self.ib_connector.ensure_connected()
            vix_val = await vix_spx.get_vix(self.ib, self.exchange_index, self.currency, self.vix_symbol,  self.ib_connector, self.logger)
            self.logger.info(f"vix value... {vix_val}")
            await self.ib_connector.ensure_connected()
            spx_quote = await vix_spx.get_spx(self.ib, self.exchange_index, self.currency, self.spx_symbol, self.ib_connector, self.logger)
            self.logger.info(f"spx_quote... {spx_quote}")
            
        except Exception as e: 
            self.logger.info(f"⚠️ Failed to fetch VIX/SPX: Exception {e}")
            
            return False, "failed to fetch VIX/SPX"
        
        spx_open = None
        try: 
            await self.ib_connector.ensure_connected()
            spx_close =await vix_spx.get_spx_close(self.ib, self.exchange_index, self.currency, self.spx_symbol, self.ib_connector, self.logger)
        except: 
            pass
        
        if spx_open:
            outside = vix_spx.rule_of_16_calculation(vix_val, spx_quote, spx_close)
            if self.config_dict['skip_backtest_vix']:
                outside = False
            if outside: 
                msg = "SPX is outside expected Range (Rule of 16)"
                return False, msg
        now = dt.datetime.now(dt.timezone.utc)
        
        #4. checking trading window
        self.logger.info("⏰ Checking the trade window timings... 🕒🔍")
        if not self.test_run:
            current_time = dt.datetime.now().time()  
            if not is_time_in_trading_windows(current_time, self.trading_windows):
                self.logger.info(f"⏰ current_time {current_time} 📊 trading windows {self.trading_windows}")
                return False, "Outside Trading window"
            
        
        return True, ""
            
        
            