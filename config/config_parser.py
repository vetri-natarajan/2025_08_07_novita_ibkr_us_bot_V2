# -*- coding: utf-8 -*-
"""
Flat config parser with separate variables for clarity
Created on Fri Aug  8 10:51:08 2025
@author: Vetriselvan
"""

import configparser
import sys
from pathlib import Path

def parse_bool(value: str) -> bool:
    value = str(value).strip().lower()
    #print(value)    
    return_value = value in ['yes', 'true', 1]
    return return_value

def parse_list(input_list : list, separator=",") -> list:
    input_list = [item for item in str(input_list).split(separator)]
    #print(input_list)
    return input_list

def get_config_inputs():
    conf = configparser.ConfigParser()
    config_path = Path("config/inputs.cfg")

    try:
        print("📂 Reading config file... 🛠️")

        if not config_path.exists():
            raise FileNotFoundError(f"{config_path} not found")

        conf.read(config_path)

        # --- SCRIP ---
        vix_symbol = conf["SCRIP"]["vix_symbol"].upper()
        spx_symbol = conf["SCRIP"]["spx_symbol"].upper()
        exchange = conf["SCRIP"]["exchange"].replace('"', '').strip()
        currency = conf["SCRIP"]["currency"].replace('"', '').strip()
        

        # --- RESTART ---
        read_restart = conf["RESTART"]["read_restart"].upper()
        auto_restart = conf["RESTART"]["auto_restart"].upper()
        

        # --- IBKR ---
        ibkr_host = conf["IBKR"]["host"]
        ibkr_port = int(conf["IBKR"]["port"])
        ibkr_client_id = int(conf["IBKR"]["client_id"])
        account_type = conf["IBKR"]["live_or_paper"].upper()


        # --- TRADING ---
        run_mode = conf["TRADING"]["run_mode"].upper()
        backtest_duration = int(conf["TRADING"]["backtest_duration"])
        backtest_duration_units = conf["TRADING"]["backtest_duration_units"]
        
        trading_time_zone = conf["TRADING"]["time_zone"]
        trading_windows = parse_list(conf["TRADING"]["trading_windows"])
        trading_capital = float(conf["TRADING"]["capital"])
        trading_units = int(conf["TRADING"]["units"])
        vix_threshold = float(conf["TRADING"]["vix_threshold"])
        vix_reduction_factor = float(conf["TRADING"]["vix_reduction_factor"])
        skip_on_high_vix = parse_bool(conf["TRADING"]["skip_on_high_vix"])
        test_run = parse_bool(conf["TRADING"]["test_run"])
        trade_time_out_secs = int(conf["TRADING"]["trade_time_out_secs"])
        auto_trade_save_secs = int(conf["TRADING"]["auto_trade_save_secs"])
        skip_backtest_vix = parse_bool(conf["TRADING"]["skip_backtest_vix"])
        
        

        # --- LOGGING ---
        log_directory = conf["LOGGING"]["log_directory"]
        config_directory = conf["LOGGING"]["config_directory"]
        trade_state_file = conf["LOGGING"]["trade_state_file"]
        trade_reporter_file = conf["LOGGING"]["trade_reporter_file"]
        order_manager_state_file = conf["LOGGING"]["order_manager_state_file"]
        data_directory = conf["LOGGING"]["data_directory"]
        backtest_directory = conf["LOGGING"]["backtest_directory"]

        # --- PREMARKET ---
        require_config_valid = parse_bool(conf["PREMARKET"]["require_config_valid"])
        loss_halt_count = int(conf["PREMARKET"]["loss_halt_count"])
        loss_halt_duration_hours = int(conf["PREMARKET"]["loss_halt_duration_hours"])

        # Build the flat dictionary
        inputs_dict = {
            "exchange": exchange,
            "currency": currency,
            "log_directory": log_directory,
            "read_restart": read_restart,
            "auto_restart": auto_restart,
            "ibkr_host": ibkr_host,
            "ibkr_port": ibkr_port,
            "ibkr_client_id": ibkr_client_id,
            "account_type": account_type,
            "vix_symbol": vix_symbol,
            "spx_symbol": spx_symbol,
            "run_mode": run_mode, 
            "backtest_duration": backtest_duration,
            "backtest_duration_units": backtest_duration_units, 
            "trading_time_zone": trading_time_zone,
            "trading_windows": trading_windows,
            "trading_capital": trading_capital,
            "trading_units": trading_units,
            "vix_threshold": vix_threshold,
            "vix_reduction_factor": vix_reduction_factor,
            "skip_on_high_vix": skip_on_high_vix,
            "test_run": test_run,
            "trade_time_out_secs" : trade_time_out_secs,
            "auto_trade_save_secs" : auto_trade_save_secs,
            "skip_backtest_vix" : skip_backtest_vix,
            "config_directory": config_directory,
            "trade_reporter_file": trade_reporter_file,
            "trade_state_file": trade_state_file,
            "order_manager_state_file": order_manager_state_file, 
            "data_directory" : data_directory,
            "backtest_directory": backtest_directory,
            "require_config_valid": require_config_valid,
            "loss_halt_count": loss_halt_count,
            "loss_halt_duration_hours": loss_halt_duration_hours
        }

        print("✅ Successfully read config inputs 📄")
        return inputs_dict

    except FileNotFoundError as e:
        print(f"⚙️ {e} 🚪 Exiting... ❌")
        sys.exit()
    except Exception as e:
        print(f"❌ Error in config parser: {e} ⚠️ 🚪 Exiting... ❌")
        sys.exit()

