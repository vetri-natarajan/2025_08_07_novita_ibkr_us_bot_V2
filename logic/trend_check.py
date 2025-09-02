import numpy as np
from logic.each_candle_gain import check_each_candle_gains
from logic.total_candle_gain import check_total_gain_limits
from logic.max_drawdown import check_max_drawdown
from logic.check_TA_confluence import check_technical_confluence

def check_HTF_conditions(symbol, main_settings, ta_settings, df_HTF, logger):
    HH_LL_bars = int(main_settings[symbol]['HHLL'])
    each_gain_limits = main_settings[symbol]['Parsed Each Gain']
    total_gain_limits = main_settings[symbol]['Parsed Total Gain']
    dd_limits = float(main_settings[symbol]['Max Draw down'].replace("%", "").strip())

    if df_HTF is None or len(df_HTF) < HH_LL_bars:
        logger.info("❌ HTF: Dataframe is None or too short")
        return False

    lastN = df_HTF.iloc[-HH_LL_bars-1:-1].copy()
    logger.info(f'HTF lastN====>\n {lastN}')

    opens = lastN['open'].astype(float)
    closes = lastN['close'].astype(float)
    highs = lastN['high'].astype(float)
    lows = lastN['low'].astype(float)

    # 1. All closes > opens (green candles)
    if not all(closes > opens):
        logger.info("❌ HTF: Not all closes > opens")
        return False

    gains = (closes - opens)*100/(opens + 1e-8)

    # 2. Each candle gain check
    if not check_each_candle_gains(gains, each_gain_limits):
        logger.info(f"❌ HTF: Gains not in range {each_gain_limits}")
        return False

    # 3. Total gain check
    if not check_total_gain_limits(gains, total_gain_limits):
        logger.info(f"❌ HTF: Total Gains condition not satisfied")
        return False

    # 4. Max drawdown
    if not check_max_drawdown(highs, lows, dd_limits):
        logger.info(f"❌ HTF: Drawdown condition failed")
        return False

    # Technical confluence check
    htf_timeframe = main_settings[symbol]["Parsed Raw TF"][0]
    if not check_technical_confluence(htf_timeframe, df_HTF, ta_settings, main_settings, logger):
        logger.info("⚠️❌ HTF technical confluence not met...")
        return False
    logger.info("✅📈 HTF all conditions are met...")
    return True
