import asyncio
import nest_asyncio
nest_asyncio.apply()

import functools
import logging
import time
from datetime import datetime, timezone

from ib_async import IB
from config.config_parser import get_config_inputs
from execution.ibkr_connector import ibkr_connector
from utils.logger import initialize_logger
from config.read_ta_settings import read_ta_settings
from config.read_watchlist_main_config import read_watchlist_main_config
from data.streaming_market_data import StreamingData  # adapt StreamingMarketData if needed
from data.historical_data_fetcher import HistoricalDataFetcher
from utils.loss_tracker import loss_tracker_class
from utils.pre_market_checks import pre_market_checks
from reports.trade_reporter import trade_reporter_class
from execution.order_manager import order_manager_class
from data.vix_spx import get_vix
from strategies.scalping_strategy import check_HTF_conditions, check_MTF_conditions, check_LTF_conditions
from execution.position_sizer import compute_qty
from indicators.atr import calculate_atr
from risk_management.atr_based_sl_tp import compute_atr_sl_tp
from risk_management.fixed_sl_tp import compute_fixed_sl_tp
from data.market_data import MarketData
from backtest.backtest import BacktestEngine, PreMarketChecksBacktest

print("""
====================================
🚀 Welcome to US-stock Bot 💹
====================================
📈 Ready to trade and grow 📊
💵 Good luck & happy trading! 💰
""")

# Load all config inputs
config_dict = get_config_inputs()
exchange = config_dict["exchange"]
currency = config_dict["currency"]
log_directory = config_dict["log_directory"]
read_restart = config_dict["read_restart"]
auto_restart = config_dict["auto_restart"]
ibkr_host = config_dict["auto_restart"]  # verify this config for typo
ibkr_port = config_dict["ibkr_port"]
ibkr_client_id = config_dict["ibkr_client_id"]
account_type = config_dict["account_type"]
run_mode = config_dict["run_mode"]
spx_symbol = config_dict["spx_symbol"]
vix_symbol = config_dict["vix_symbol"]
backtest_duration = config_dict["backtest_duration"]
backtest_duration_units = config_dict["backtest_duration_units"]
trading_time_zone = config_dict["trading_time_zone"]
trading_windows = config_dict["trading_windows"]
trading_capital = config_dict["trading_capital"]
trading_units = config_dict["trading_units"]
vix_threshold = config_dict["vix_threshold"]
vix_reduction_factor = config_dict["vix_reduction_factor"]
skip_on_high_vix = config_dict["skip_on_high_vix"]
test_run = config_dict["test_run"]
trade_time_out_secs = config_dict["trade_time_out_secs"]
auto_trade_save_secs = config_dict["auto_trade_save_secs"]
log_level = config_dict["log_level"]
log_file = config_dict["log_file"]
config_directory = config_dict["config_directory"]
trade_state_file = config_dict["trade_state_file"]
trade_reporter_file = config_dict["trade_reporter_file"]
order_manager_state_file = config_dict["order_manager_state_file"]
data_directory = config_dict["data_directory"]
require_config_valid = config_dict["require_config_valid"]
loss_halt_count = config_dict["loss_halt_count"]
loss_halt_duration_hours = config_dict["loss_halt_duration_hours"]

logger = initialize_logger(config_dict["log_directory"], "US_stocks", logging.INFO, config_dict["run_mode"])
symbol_list, watchlist_main_settings = read_watchlist_main_config("config/Symbol_WatchList_Main_Configuration.csv", logger)
print(watchlist_main_settings)
ib = IB()

# Signal caches 
htf_signals = {}
mtf_signals = {}

async def run_backtest_entrypoint(ib, account_value):    
    logger.info("🔍📈 Entering backtest module")
    import datetime as dt
    # Define your backtest period end and duration
    end_time = dt.datetime.now()
    duration_value = backtest_duration  # from config, e.g., 2
    duration_unit = backtest_duration_units  # from config, e.g., 'weeks'

    # Initialize HistoricalDataFetcher
    fetcher = HistoricalDataFetcher(
        ib=ib,
        config_dict = config_dict, 
        watchlist_main_settings=watchlist_main_settings,
        logger = logger,
        data_dir = data_directory # or your preferred directory
        
    )

    # Compute start_time from config durations
    start_time = fetcher.get_start_time(end_time, duration_value, duration_unit)

    # Fetch historical VIX and SPX data needed for pre-market checks
    print("start_time ===>", start_time)
    print("end_time ===>", end_time)

    historical_vix_df = await fetcher.fetch_vix_data(start_time, end_time)
    historical_spx_df = await fetcher.fetch_spx_data(start_time, end_time)
    
    print(len(historical_vix_df))
    #print(historical_vix_df.head())
    print(historical_vix_df)
    print(len(historical_spx_df))
    #print(historical_spx_df.head())
    print(historical_spx_df)
    
    # Initialize premarket checker with these dataframes
    premarket_bt_checker = PreMarketChecksBacktest(
        historical_vix=historical_vix_df,
        historical_spx=historical_spx_df,
        config_dict=config_dict,
        logger=logger
    )

    # Initialize backtester with fetcher and premarket checks
    backtester = BacktestEngine(
        symbol_list,
        account_value,
        fetcher,
        config_dict,
        premarket_bt_checker,
        logger
    )

    # Run the backtest
    await backtester.run_backtest(
        config_dict,
        symbol_list,
        watchlist_main_settings,
        duration_value,
        duration_unit,
        end_time
    )



async def process_trading_signals_cached(symbol, df_HTF, df_MTF, df_LTF,
                                         market_data, order_manager, cfg,
                                         account_value, vix, logger,
                                         watchlist_main_settings, ta_settings,
                                         max_look_back, trading_units,
                                         vix_threshold, vix_reduction_factor,
                                         skip_on_high_vix):
    if not htf_signals.get(symbol, False):
        logger.info(f"⏸️ HTF conditions not met for {symbol}")
        return False
    if not mtf_signals.get(symbol, False):
        logger.info(f"⏸️ MTF conditions not met for {symbol}")
        return False
    okLTF = check_LTF_conditions(symbol, watchlist_main_settings, ta_settings, max_look_back, df_LTF, df_HTF, logger)
    if not okLTF:
        logger.info(f"⏸️ LTF conditions not met for {symbol}")
        return False

    last_price = df_LTF['close'].iloc[-1]
    qty = compute_qty(account_value, trading_units, last_price, vix, vix_threshold, vix_reduction_factor, skip_on_high_vix)
    if qty <= 0:
        logger.info(f"⚠️ Qty zero for {symbol}")
        return False

    exit_method = watchlist_main_settings[symbol]['Exit']
    sl_input = watchlist_main_settings[symbol]['SL']
    tp_input = watchlist_main_settings[symbol]['TP']
    if exit_method == "E1":
        sl_price, tp_price = compute_fixed_sl_tp(last_price, sl_input, tp_input)
    elif exit_method == "E2":
        try:
            atr_val_df = calculate_atr(df_LTF, max_look_back * 2)
            atr_val = float(atr_val_df.iloc[-1])
        except Exception:
            atr_val = None
        if atr_val is not None:
            sl_price, tp_price = compute_atr_sl_tp(last_price, atr_val, sl_input, tp_input)
        else:
            sl_price, tp_price = compute_fixed_sl_tp(last_price, sl_input, tp_input)
    else:
        sl_price, tp_price = compute_fixed_sl_tp(last_price, sl_input, tp_input)

    contract = market_data._subscribed.get(symbol, {}).get('contract')
    if contract is None and hasattr(order_manager, 'get_contract'):
        contract = order_manager.get_contract(symbol)
    if contract is None:
        logger.warning(f"No contract for {symbol}, skip.")
        return False

    if order_manager.has_active_trades_for_symbol(symbol):
        logger.info(f"Active trade exists for {symbol}, skipping.")
        return False

    meta = {'signal': okLTF, 'symbol': symbol}
    trade_id = await order_manager.place_market_entry_with_bracket(contract, qty, 'BUY', sl_price, tp_price, meta)
    if trade_id:
        logger.info(f"✅ Trade placed {trade_id} for {symbol} qty {qty}")
    return trade_id is not None

async def on_bar_handler(symbol, timeframe, df, market_data, order_manager, cfg, account_value, vix, logger):
    HTF, MTF, LTF = watchlist_main_settings[symbol]['Parsed TF']

    ta_settings, max_look_back = read_ta_settings(symbol, config_directory, logger)
    logger.info(f"TA settings loaded for {symbol}")

    if timeframe == HTF:
        htf_signals[symbol] = check_HTF_conditions(symbol, watchlist_main_settings, ta_settings, max_look_back, df, logger)
        logger.info(f"HTF({HTF}) signal updated for {symbol}: {htf_signals[symbol]}")
    elif timeframe == MTF:
        mtf_signals[symbol] = check_MTF_conditions(symbol, watchlist_main_settings, ta_settings, max_look_back, df, logger)
        logger.info(f"MTF({MTF}) signal updated for {symbol}: {mtf_signals[symbol]}")
    elif timeframe == LTF:
        df_HTF = market_data.get_latest(symbol, HTF)
        df_MTF = market_data.get_latest(symbol, MTF)
        df_LTF = df
        if df_HTF is None or df_MTF is None or df_LTF is None:
            logger.info(f"⏳ Data not ready for {symbol}")
            return
        await process_trading_signals_cached(symbol, df_HTF, df_MTF, df_LTF, market_data,
                                            order_manager, cfg, account_value, vix,
                                            logger, watchlist_main_settings, ta_settings,
                                            max_look_back, trading_units=cfg["trading_units"],
                                            vix_threshold=cfg["vix_threshold"],
                                            vix_reduction_factor=cfg["vix_reduction_factor"],
                                            skip_on_high_vix=cfg["skip_on_high_vix"])

async def run_live_mode(ib_connector):
    account_value = trading_capital
    market_data = MarketData(ib)
    streaming_data = StreamingData(ib, logger)
    loss_tracker = loss_tracker_class(loss_halt_count, loss_halt_duration_hours, trade_state_file)
    pre_market = pre_market_checks(ib, config_dict, loss_tracker, vix_symbol, spx_symbol, logger)
    trade_reporter = trade_reporter_class(trade_reporter_file, logger)
    order_manager = order_manager_class(ib, trade_reporter, loss_tracker, order_manager_state_file,
                                        trade_time_out_secs, auto_trade_save_secs, config_dict, logger)
    await order_manager.load_state(market_data, ib)

    passed, reason = await pre_market.run_checks()
    if not passed:
        logger.error(f"❌ Pre-market checks failed: {reason}")
        return
    logger.info("✅ All Pre-market checks passed")

    try:
        vix = await get_vix(ib, exchange, currency, vix_symbol, logger)
    except Exception:
        vix = vix_threshold

    for symbol in symbol_list:
        contract = ib_connector.create_stock_contract(symbol, exchange, currency)
        parsed_tf = watchlist_main_settings[symbol]['Parsed TF']
        ta_settings, max_look_back = read_ta_settings(symbol, config_directory, logger)
        max_tf = parsed_tf[0]
        for tf in parsed_tf:
            subscribed = await streaming_data.subscribe(contract, tf, max_tf, max_look_back)
            if not subscribed:
                logger.warning(f"⚠️ Subscription failed for {symbol} {tf}")
                continue
            callback = functools.partial(on_bar_handler, symbol=symbol, timeframe=tf,
                                         market_data=streaming_data, order_manager=order_manager,
                                         cfg=config_dict, account_value=account_value, vix=vix, logger=logger)
            streaming_data.on_bar(symbol, tf, callback)
            logger.info(f"✅ Subscribed and set handler for {symbol} {tf}")

    logger.info("🚀 Streaming subscriptions active. Awaiting bars...")

    try:
        while True:
            await asyncio.sleep(60)
    finally:
        await streaming_data.close()
        await ib.disconnectAsync()

async def main():
    ib_connector = ibkr_connector(account_type, ib, ibkr_client_id)
    asyncio.create_task(ib_connector.connect())

    while not ib.isConnected():
        await asyncio.sleep(0.2)

    logger.info(f"⚙️ Run mode: {run_mode}")

    if run_mode == "BACKTEST":
        account_value = trading_capital
        await run_backtest_entrypoint(ib, account_value)
    else:
        await run_live_mode(ib_connector)

if __name__ == '__main__':
    asyncio.run(main())