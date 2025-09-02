import numpy as np
from logic.check_TA_confluence import check_technical_confluence
def check_MTF_conditions(symbol, main_settings, ta_settings, max_look_back,  df_MTF, logger):
    if df_MTF is None:
        logger.info("❌ MTF : Dataframe is None or too short")
        return False

    HH_LL_bars = int(main_settings[symbol]['HHLL'])
    parsed_HTF = main_settings[symbol]["Parsed TF"][0]
    parsed_MTF = main_settings[symbol]["Parsed TF"][1]

    mtf_look_back = 0
    if parsed_HTF == '30 mins' and parsed_MTF == '5 mins':
        mtf_look_back = HH_LL_bars * 6
    elif parsed_HTF == '1 week' and parsed_MTF == '1 day':
        mtf_look_back = HH_LL_bars * 5
    else:
        logger.info("❌ MTF Higher and medium time frame mismatch detected 🚩")
        raise ValueError("❌ MTF Higher and medium time frame mismatch detected 🚩")

    lastN = df_MTF.iloc[-(mtf_look_back + 1): -1].copy()
    logger.info(f'MTF lastN====>\n {lastN}')
    opens = lastN['open'].astype(float)
    closes = lastN['close'].astype(float)
    highs = lastN['high'].astype(float)
    lows = lastN['low'].astype(float)

    # 1. volatility check
    vol_percent = (highs - lows) * 100 / lows
    vol_filters = main_settings[symbol]['Parsed Each Volatility']
    vol_filter_lower = vol_filters[0]
    vol_filter_upper = vol_filters[1]

    if not all((vol_filter_lower < vol_percent) & (vol_filter_upper > vol_percent)):
        logger.info(f"❌ MTF Volatility is not within the range: {vol_filter_lower} to {vol_filter_upper}")
        return False

    # 2. wick body ratio check
    upper_wick = highs - np.maximum(opens, closes)
    body = np.abs(closes - opens)
    upper_wick_body_ratio = upper_wick / (body + 1e-8)
    if (upper_wick_body_ratio > 2.5).any():
        logger.info("❌ MTF upper_wick_body_ratio is greater than 2.5")
        return False

    # 3. wick ratio check
    total_range = highs - lows
    upper_wick_ratio = upper_wick / (total_range + 1e-8)
    if (upper_wick_ratio > 0.6).any():
        logger.info("❌ MTF upper_wick_ratio is greater than 0.6")
        return False

    # 4. parabolic gain check
    gains = (closes - opens) * 100 / (opens + 1e-8)
    body_ratio = body / (total_range + 1e-8)
    if ((gains > 8) & (body_ratio > 0.8)).any():
        logger.info("❌ MTF Parabolic candles present")
        return False

    # Technical confluence
    mtf_timeframe = main_settings[symbol]["Parsed Raw TF"][1]
    
    technical_confluence = check_technical_confluence(mtf_timeframe, df_MTF, ta_settings, main_settings, logger)
    if not technical_confluence:
        logger.info("⚠️❌ MTF technical confluence not met...")
        return False
    logger.info("✅📈 MTF all conditions are met...")

    return True
