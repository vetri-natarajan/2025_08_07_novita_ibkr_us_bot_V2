import asyncio
import nest_asyncio
nest_asyncio.apply()

import os
import math
import logging
from datetime import datetime
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
from strategies.strategy import check_HTF_conditions, check_MTF_conditions, check_LTF_conditions, check_TA_confluence
from execution.position_sizer import compute_qty
from indicators.atr import calculate_atr
from risk_management.atr_based_sl_tp import compute_atr_sl_tp
from risk_management.fixed_sl_tp import compute_fixed_sl_tp
from data.market_data import MarketData
from backtest.backtest import BacktestEngine, PreMarketChecksBacktest
from utils.is_in_trading_window import is_time_in_trading_windows
from utils.make_path import make_path
print("""
====================================
🚀 Welcome to US-stock Bot 💹
====================================
📈 Ready to trade and grow 📊
💵 Good luck & happy trading! 💰
""", flush=True)

# Load all config inputs
config_dict = get_config_inputs()
exchange = config_dict["exchange"]
exchange_index = config_dict["exchange_index"]
currency = config_dict["currency"]
log_directory = config_dict["log_directory"]
read_restart = config_dict["read_restart"]
auto_restart = config_dict["auto_restart"]
tws_or_gateway = config_dict["tws_or_gateway"]
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
percent_of_account_value = config_dict["percent_of_account_value"] 
trading_units = config_dict["trading_units"]
vix_threshold = config_dict["vix_threshold"]
vix_reduction_factor = config_dict["vix_reduction_factor"]
skip_on_high_vix = config_dict["skip_on_high_vix"]
test_run = config_dict["test_run"]
order_testing = config_dict["order_testing"]
trade_time_out_secs = config_dict["trade_time_out_secs"]
auto_trade_save_secs = config_dict["auto_trade_save_secs"]
inputs_directory = config_dict["inputs_directory"]
main_configuration_file = config_dict["main_configuration_file"]
trade_state_file = config_dict["trade_state_file"]
trade_reporter_file = config_dict["trade_reporter_file"]
order_manager_state_file = config_dict["order_manager_state_file"]
data_directory = config_dict["data_directory"]
backtest_directory = config_dict["backtest_directory"]
require_config_valid = config_dict["require_config_valid"]
loss_halt_count = config_dict["loss_halt_count"]
loss_halt_duration_hours = config_dict["loss_halt_duration_hours"]

logger = initialize_logger(config_dict["log_directory"], "US_stocks", logging.INFO, config_dict["run_mode"])
main_configuration_file_path = make_path(inputs_directory, main_configuration_file)
symbol_list, watchlist_main_settings = read_watchlist_main_config(main_configuration_file_path, logger)
print(watchlist_main_settings)

ib = IB()
# Signal caches 
htf_signals = {}
mtf_signals = {}


ALWAYS_TFS = ["1 week", "1 day", "4 hours", "1 hour", "30 mins", "15 mins", "5 mins", "1 min"]  # labels must match StreamingData [attached_file:1][attached_file:2]

async def run_backtest_entrypoint(ib, account_value, ib_connector):    
    logger.info("🔍📈 Entering backtest module")
    import datetime as dt
    end_time = dt.datetime.now()
    duration_value = backtest_duration
    duration_unit = backtest_duration_units
    fetcher = HistoricalDataFetcher(
        ib=ib,
        config_dict=config_dict,
        watchlist_main_settings=watchlist_main_settings,
        logger=logger,
        ib_connector = ib_connector,
        data_dir=data_directory
    )
    start_time = fetcher.get_start_time(end_time, duration_value, duration_unit)
    logger.info(f"⏰ start_time ===> {start_time}")
    logger.info(f"⏰ end_time ===> {end_time}")

    historical_vix_df = await fetcher.fetch_vix_data(start_time, end_time)
    historical_spx_df = await fetcher.fetch_spx_data(start_time, end_time)
    logger.info(len(historical_vix_df))
    logger.info(historical_vix_df)
    logger.info(len(historical_spx_df))
    logger.info(historical_spx_df)
    premarket_bt_checker = PreMarketChecksBacktest(
        historical_vix=historical_vix_df,
        historical_spx=historical_spx_df,
        config_dict=config_dict,
        logger=logger
    )
    
    trade_state_file_bt = trade_state_file + "_bakctest"  
    logger.info(f"🔍 Checking whether previous backtest loss tracker file exists...")
    if os.path.exists(trade_state_file_bt):
        os.remove(trade_state_file_bt)
        logger.info(f"♻️ Removed old backtest loss tracker file {trade_state_file_bt} — starting fresh logging!")

    else: 
        logger.info(f"🆕 No previous loss tracker file found ({trade_state_file_bt}). Creating a fresh one now.")
    loss_tracker = loss_tracker_class(loss_halt_count, loss_halt_duration_hours, trade_state_file_bt, logger)
    
    backtester = BacktestEngine(
        ib,
        ALWAYS_TFS,
        symbol_list,
        account_value,
        fetcher,
        config_dict,
        premarket_bt_checker,
        watchlist_main_settings, 
        loss_tracker,
        logger
    )
    await backtester.run_backtest(
        config_dict,
        symbol_list,
        watchlist_main_settings,
        duration_value,
        duration_unit,
        end_time
    )

async def process_trading_signals_cached(symbol_combined, symbol, timeframe,  skip_LTF, df_HTF, df_MTF, df_LTF,
                                         streaming_data, order_manager, cfg,
                                         account_value, percent_of_account_value, vix, logger,
                                         watchlist_main_settings, ta_settings,
                                         max_look_back, trading_units,
                                         vix_threshold, vix_reduction_factor,
                                         skip_on_high_vix):
    current_time = datetime.now().time()  
    if (is_time_in_trading_windows(current_time, trading_windows)) or test_run:
    
        if not htf_signals.get(symbol_combined, False):
            logger.info(f"⏸️ HTF conditions not met for {symbol_combined}")
            return False
        if not mtf_signals.get(symbol_combined, False):
            logger.info(f"⏸️ MTF conditions not met for {symbol_combined}")
            return False
        live_price = None
        ticker = streaming_data.tickers.get(symbol)
        if ticker is not None: 
            live_price = ticker.last
        is_live = True
        
        if not skip_LTF: 
            okLTF = check_LTF_conditions(symbol_combined, symbol, watchlist_main_settings, ta_settings, max_look_back, df_LTF, df_HTF, logger, order_testing, is_live, live_price)
            if not okLTF:
                logger.info(f"⏸️ LTF conditions not met for {symbol_combined}")
                return False
        else:
            okLTF = True
        data_cache_symbol = streaming_data._data_cache[symbol]
        
        #logger.info(f'data_cahe_symbol --->{data_cache_symbol}')
        
        okTA = check_TA_confluence(symbol_combined, symbol, ALWAYS_TFS, data_cache_symbol, ta_settings , watchlist_main_settings, logger, is_live)
        
        if not okTA:
            logger.info(f"⏸️ TA confluence conditions not met for {symbol_combined}")
            return False            
        
        ticker = streaming_data.tickers.get(symbol)
        last_price = ticker.last
        
        while (isinstance(last_price, float) and math.isnan(last_price)):            
            logger.info("⚠️ Last Price is NaN — probably this is the first tick... ⏳ Waiting to receive tick.")
            await asyncio.sleep(0.5)
            ticker = streaming_data.tickers.get(symbol)
            last_price = ticker.last

        
        '''
        if ticker is not None: 
            last_price = ticker.last
        
        else: 
            last_price = df_LTF['close'].iloc[-1]        
        
        '''

            
            
        
        if order_testing:
            if symbol == "INFY":
                last_price = 1513
            elif symbol == "SBIN":
                last_price = 937
            elif symbol == "RELIANCE":
                last_price = 1487
                
        floating_equity = order_manager.get_floating_equity()
        '''
        logger.info(
            f"📊 Floating Equity: {floating_equity}\n"
            f"💰 Percent of Account Value: {percent_of_account_value}\n"
            f"📈 Trading Units: {trading_units}\n"
            f"💹 ticker: {ticker}\n"
            f"💹 Last Price: {last_price}\n"
            f"🌪️ VIX: {vix}\n"
            f"⚠️ VIX Threshold: {vix_threshold}\n"
            f"📉 VIX Reduction Factor: {vix_reduction_factor}\n"
            f"🚫 Skip on High VIX: {skip_on_high_vix}\n"
)
        '''

        qty = compute_qty(floating_equity, percent_of_account_value, trading_units, last_price, vix, logger, vix_threshold, vix_reduction_factor, skip_on_high_vix)

            
        if qty <= 0:
            logger.info(f"⚠️ Qty zero for {symbol}")
            return False
        exit_method = watchlist_main_settings[symbol_combined]['Exit']
        sl_input = watchlist_main_settings[symbol_combined]['SL']
        tp_input = watchlist_main_settings[symbol_combined]['TP']
        
        logger.info(f"🧮 Computed qty for [{symbol}] qty: {qty}")
        special_exit = False # for E3 and E4 exits
        if exit_method == "E1":
            sl_price, tp_price = compute_fixed_sl_tp(last_price, sl_input, tp_input)
            #logger.info(f'🧾 Initial exit calculation: exit_method {exit_method}, 🛑 sl_price: {sl_price}, 🎯 tp_price: {tp_price}')
        elif exit_method == "E2":
            try:
                atr_val_df = calculate_atr(df_LTF, 5)
                atr_val = float(atr_val_df.iloc[-1])
            except Exception:
                atr_val = None
            if atr_val is not None:
                sl_price, tp_price = compute_atr_sl_tp(last_price, atr_val, sl_input, tp_input)
            else:
        
                sl_price, tp_price = compute_fixed_sl_tp(last_price, sl_input, tp_input)
        elif exit_method in ['E3', 'E4']:
            sl_input = 2 # 2 percetn
            tp_input = 4 # 4 percent
            sl_price, tp_price = compute_fixed_sl_tp(last_price, sl_input, tp_input) #initially place large sl and tp so they don't fill  then modify it
            special_exit = True
        else:
            logger.info("⚠️ Exit method not defined, taking default fixed_sl_exits.")
            sl_price, tp_price = compute_fixed_sl_tp(last_price, sl_input, tp_input)
        logger.info(f" Exit method for [{symbol}] Exit method: {exit_method} sl: {sl_price} tp: {tp_price}")  
        
        contract = streaming_data._subscriptions.get(symbol, {}).get('5 secs', {}).get('contract')
        logger.info(f'Contract for [{symbol}] {contract}')
        if contract is None and hasattr(order_manager, 'get_contract'):
            contract = order_manager.get_contract(symbol)
        if contract is None:
            logger.warning(f"No contract for {symbol}, skip.")
            return False
        if order_manager.has_active_trades_for_symbol(symbol_combined, symbol):
            logger.info(f"Active trade exists for {symbol}, skipping.")
            return False
        meta = {'signal': okLTF, 'symbol': symbol}
        
        trade_id = await order_manager.place_market_entry_with_bracket( symbol_combined, symbol,                                                     
                                                                       contract, 
                                                                       qty, 
                                                                       'BUY',
                                                                       df_LTF, 
                                                                       sl_price, 
                                                                       tp_price, 
                                                                       meta, 
                                                                       last_price, 
                                                                       order_testing, 
                                                                       special_exit
                                                                       )
        if trade_id:
            logger.info(f"✅ Trade placed {trade_id} for {symbol} qty {qty}")
        return trade_id is not None
    else: 
        logger.info(f"⏰ current_time {current_time} not in trading windows {trading_windows} 🚫")

async def on_bar_handler(symbol, timeframe, df, symbol_combined, streaming_data, ta_settings, mode, max_look_back, order_manager, cfg, account_value, vix, logger):
    HTF, MTF, LTF = watchlist_main_settings[symbol_combined]['Parsed TF']
    skip_HTF = watchlist_main_settings[symbol_combined]['Skip HTF'].strip().upper() in ['Y', 'YES', 'TRUE', '1' ]
    skip_MTF = watchlist_main_settings[symbol_combined]['Skip MTF'].strip().upper() in ['Y', 'YES', 'TRUE', '1' ]
    skip_LTF = watchlist_main_settings[symbol_combined]['Skip LTF'].strip().upper() in ['Y', 'YES', 'TRUE', '1' ]
    
    #logger.info(f"TA settings loaded for {symbol}")
    
           
    if timeframe == HTF:    
        if not skip_HTF:         
            if not order_testing:
                htf_signals[symbol_combined] = check_HTF_conditions(symbol_combined, symbol, watchlist_main_settings, ta_settings, max_look_back, df, logger)
            else:
                htf_signals[symbol_combined] = True                

        else:        
            htf_signals[symbol_combined] = True            
            logger.info(f"⏭️🕰️ Skip HTF enabled — skipping HTF checksfor [{symbol_combined}] ✅")
        
        signal_htf = htf_signals[symbol_combined]
        
        
        if signal_htf:
            await streaming_data.subscribe_live_ticks(symbol)
            logger.info(f"🔔 Live tick subscribed for {symbol} on HTF signal")      
        '''
        else: 
            await streaming_data.unsubscribe_live_ticks(symbol)
            logger.info(f"🔕 Live tick unsubscribed for {symbol}")   
        '''

            
        emoji = "✅" if signal_htf else "❌"
        logger.info(f"{emoji} HTF({HTF}) signal updated for {symbol_combined}: {signal_htf}")
        
    elif timeframe == MTF:
        if not skip_MTF:
            if not order_testing:
                mtf_signals[symbol_combined] = check_MTF_conditions(symbol_combined, symbol, watchlist_main_settings, ta_settings, max_look_back, df, logger)
            
            else:
                mtf_signals[symbol_combined] = True
                
            signal_mtf = mtf_signals[symbol_combined]
            emoji = "✅" if signal_mtf else "❌"
            logger.info(f"{emoji} MTF({MTF}) signal updated for {symbol_combined}: {signal_mtf}")
        
        else: 
            mtf_signals[symbol_combined] = True
            logger.info(f"⏭️🕰️ Skip MTF enabled — skipping {MTF} checksfor [{symbol_combined}] ✅")
            
    elif timeframe == LTF:
        df_HTF = streaming_data.get_latest(symbol, HTF)
        df_MTF = streaming_data.get_latest(symbol, MTF)
        df_LTF = df
        
        if not skip_LTF:            
            if df_HTF is None and not skip_HTF: 
                logger.info(f"⏳ HTF data not ready for {symbol_combined}")
                return

            if df_MTF is None and not skip_MTF: 
                logger.info(f"⏳ MTF data not ready for {symbol_combined}")
                return

            if df_LTF is None and not skip_LTF: 
                logger.info(f"⏳ LTF data not ready for {symbol_combined}")
                return            
            await process_trading_signals_cached(symbol_combined, symbol, timeframe,  skip_LTF, df_HTF, df_MTF, df_LTF, streaming_data,
                                                order_manager, cfg, account_value, percent_of_account_value, vix,
                                                logger, watchlist_main_settings, ta_settings,
                                                max_look_back, trading_units=cfg["trading_units"],
                                                vix_threshold=cfg["vix_threshold"],
                                                vix_reduction_factor=cfg["vix_reduction_factor"],
                                                skip_on_high_vix=cfg["skip_on_high_vix"])
        else:
            logger.info(f"⏭️🕰️ Skip LTF enabled — skipping {LTF} checksfor [{symbol_combined}] ✅")
            await process_trading_signals_cached(symbol_combined, symbol, timeframe, skip_LTF, df_HTF, df_MTF, df_LTF, streaming_data,
                                                order_manager, cfg, account_value, percent_of_account_value, vix,
                                                logger, watchlist_main_settings, ta_settings,
                                                max_look_back, trading_units=cfg["trading_units"],
                                                vix_threshold=cfg["vix_threshold"],
                                                vix_reduction_factor=cfg["vix_reduction_factor"],
                                                skip_on_high_vix=cfg["skip_on_high_vix"])
        
async def poll_partial_bars(streaming_data: StreamingData, symbol: str, timeframe: str):
    while True:
        partial_bar = streaming_data.get_latest_partial_bar(symbol, timeframe)
        if partial_bar:
            streaming_data.logger.info(f"Live partial bar [{symbol}][{timeframe}]: {partial_bar}")
        else:
            streaming_data.logger.info(f"No partial bar found for [{symbol}][{timeframe}]")
        await asyncio.sleep(5)

async def run_live_mode(ib_connector):
    await ib_connector.ensure_connected()
    
    account_value = trading_capital
    market_data = MarketData(ib)
    streaming_data = StreamingData(ib, logger, trading_time_zone, ib_connector)
    loss_tracker = loss_tracker_class(loss_halt_count, loss_halt_duration_hours, trade_state_file, logger)
    pre_market = pre_market_checks(ib, config_dict, loss_tracker, vix_symbol, spx_symbol, ib_connector,  logger)
    trade_reporter = trade_reporter_class(trade_reporter_file, logger)
    order_manager = order_manager_class(ib, trade_reporter, loss_tracker, order_manager_state_file,
                                        trade_time_out_secs, auto_trade_save_secs, config_dict, watchlist_main_settings, streaming_data, ib_connector, logger)
    await order_manager.load_state(market_data, ib)
    order_manager.set_realized_cash(float(trading_capital))
    passed, reason = await pre_market.run_checks()
    if not passed:
        logger.error(f"❌ Pre-market checks failed: {reason}")
        return
    logger.info("✅ All Pre-market checks passed")
    try:
        vix = await get_vix(ib, exchange, currency, vix_symbol, ib_connector, logger)
    except Exception:
        vix = vix_threshold
        
    #subscribe to all the time frames
    # Fixed timeframe set for universal subscription (parsed_tf is a subset of these)

    for symbol_combined in symbol_list:
        symbol, mode = symbol_combined.split('_')
        logger.info(f"📌 symbol: {symbol}    ⚙️ mode: {mode}")
        contract = ib_connector.create_stock_contract(symbol, exchange, currency)
        qualified = ib.qualifyContracts(contract)
        logger.info(f"✅ Qualified Contract: {qualified[0]}")
        logger.info(f"📌 conId: {qualified[0].conId}")
        
        parsed_tf = watchlist_main_settings[symbol_combined]['Parsed TF']
        ta_settings, max_look_back = read_ta_settings(symbol_combined, symbol, mode, inputs_directory, watchlist_main_settings, logger)
        max_tf = parsed_tf[0]

        # Subscribe the fixed set for data availability (independent of parsed_tf)
        for tf in ALWAYS_TFS:
            logger.info(f"📡 Subscribing for symbol: [{symbol}] ⏱️ timeframe: {tf}")
            subscribed = await streaming_data.subscribe(contract, tf, max_tf, max_look_back, order_testing)
            if not subscribed:
                logger.warning(f"Subscription failed for {symbol} {tf}")
                continue
    
    for symbol_combined in symbol_list:
        symbol, mode = symbol_combined.split('_')
        logger.info(f"📌 symbol: {symbol}    ⚙️ mode: {mode}")
        contract = ib_connector.create_stock_contract(symbol, exchange, currency)
        qualified = ib.qualifyContracts(contract)
        logger.info(f"✅ Qualified Contract: {qualified[0]}")
        logger.info(f"📌 conId: {qualified[0].conId}")
        
        parsed_tf = watchlist_main_settings[symbol_combined]['Parsed TF']
        ta_settings, max_look_back = read_ta_settings(symbol_combined, symbol, mode, inputs_directory, watchlist_main_settings, logger)
        max_tf = parsed_tf[0]
        #ta_settings, max_look_back = read_ta_settings(symbol, config_directory, logger)
    
        for tf in parsed_tf:
            subscribed = await streaming_data.subscribe(contract, tf, max_tf, max_look_back, order_testing)
            if not subscribed:
                logger.warning(f"⚠️ Subscription failed for {symbol} {tf}")
                continue
            # Register callback wrapper
            async def on_bar_handler_wrapper(symbol_inner, timeframe_inner, df_inner, tf=tf):
                await on_bar_handler(symbol_inner, timeframe_inner, df_inner,  
                                     symbol_combined = symbol_combined,
                                     streaming_data=streaming_data,
                                     ta_settings = ta_settings, 
                                     mode = mode,
                                     max_look_back = max_look_back,
                                     order_manager=order_manager,
                                     cfg=config_dict,
                                     account_value=account_value,
                                     vix=vix,
                                     logger=logger)
            streaming_data.on_bar(symbol, tf, on_bar_handler_wrapper)
            #asyncio.create_task(poll_partial_bars(streaming_data, symbol, tf))

            # Immediately fire the callback with seeded historical bars to check initial conditions
            seeded_df = streaming_data.get_latest(symbol, tf)
            if (seeded_df is not None and not seeded_df.empty) or order_testing:
                # Run callback synchronously here (no live bar yet, so dummy event loop sync)
                asyncio.create_task(on_bar_handler_wrapper(symbol, tf, seeded_df))
            else:
                logger.warning(f"⚠️ No seeded historical data to trigger initial callback for {symbol} [{tf}]")



    logger.info(f"✅ Subscribed and set handlers for all timeframes for {symbol}")
    try:
        while True:
            await ib_connector.ensure_connected()
            await asyncio.sleep(60)
    finally:
        await streaming_data.close()
        await ib.disconnectAsync()


async def main():
    ib_connector = ibkr_connector(tws_or_gateway, account_type, ib, ibkr_client_id, logger)
    asyncio.create_task(ib_connector.connect())

    await ib_connector.ensure_connected()
    
    logger.info(f"⚙️ Run mode: {run_mode}")
    if run_mode == "BACKTEST":
        account_value = trading_capital
        await run_backtest_entrypoint(ib, account_value, ib_connector)
    else:
        await run_live_mode(ib_connector)

if __name__ == '__main__':
    asyncio.run(main())