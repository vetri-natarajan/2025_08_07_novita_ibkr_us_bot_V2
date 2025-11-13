import numpy as np


from logic.helper_modules import(
    extract_ohlc_as_float,
    check_htf_structure,
    calculate_gains,
    check_each_candle_gains,
    check_total_gain_limits,
    check_max_drawdown,
    check_technical_confluence
    )


def check_HTF_conditions(symbol_combined, symbol, main_settings, ta_settings, max_look_back, df_HTF, logger):
    HH_LL_bars = int(main_settings[symbol_combined]['HHLL'])
    each_gain_limits = main_settings[symbol_combined]['Parsed Each Gain']
    total_gain_limits = main_settings[symbol_combined]['Parsed Total Gain']
    dd_limits = float(main_settings[symbol_combined]['Max Draw down'].replace("%", "").strip())

    if df_HTF is None or len(df_HTF) < (HH_LL_bars + 1):
        logger.info("❌ HTF: Dataframe is None or too short")
        return False
    logger.info(f"➡️ dt HTF [{symbol_combined}]  ====>\n {df_HTF}")
    lastN = df_HTF.iloc[-HH_LL_bars-1:-1].copy()
    logger.info(f"➡️ HTF lastN [{symbol_combined}] ====>\n {lastN}")

    
    # Extract typed OHLC series for vectorized computations.
    opens, highs, lows, closes = extract_ohlc_as_float(lastN)

    # 1. All closes > opens (green candles)
    if not check_htf_structure(opens, closes, logger):        
        return False
    
    #calculate gains 
    gains = calculate_gains(opens, closes)

    # 2. Each candle gain check
    if not check_each_candle_gains(gains, each_gain_limits, logger):        
        return False

    # 3. Total gain check
    if not check_total_gain_limits(gains, total_gain_limits, logger):
        return False

    # 4. Max drawdown
    if not check_max_drawdown(highs, lows, dd_limits, logger):        
        return False

    '''
    # Technical confluence check
    htf_timeframe = main_settings[symbol_combined]["Parsed Raw TF"][0]
    if not check_technical_confluence(htf_timeframe, df_HTF, ta_settings, main_settings, logger):
        logger.info("⚠️❌ HTF technical confluence for symbol {symbol_combined} not met...")
        return False
    '''
    logger.info(f"✅📈 HTF all conditions for symbol {symbol_combined} are met...")
    return True
