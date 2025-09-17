# -*- coding: utf-8 -*-
"""
Created on Tue Sep  2 08:23:18 2025

@author: Vetriselvan
"""

# The technical confluence function can remain here or similarly refactored later
def check_technical_confluence(timeframe, df_TF, ta_settings, main_settings, logger) -> bool:
    yes_list = ["Y", "YES", "TRUE", "1"]
    logger.info(ta_settings["RSI"]["other_columns"][timeframe].upper())
    check_rsi = ta_settings["RSI"]["other_columns"][timeframe].upper() in yes_list
    check_macd = ta_settings["MACD"]["other_columns"][timeframe].upper() in yes_list
    check_vwap = ta_settings["VWAP"]["other_columns"][timeframe].upper() in yes_list
    check_ema = ta_settings["EMA"]["other_columns"][timeframe].upper() in yes_list
    check_adx = ta_settings["ADX"]["other_columns"][timeframe].upper() in yes_list
    check_atr = ta_settings["ATR"]["other_columns"][timeframe].upper() in yes_list
    check_obv = ta_settings["OBV"]["other_columns"][timeframe].upper() in yes_list

    from indicators.rsi import calculate_rsi
    from indicators.macd import calculate_macd
    from indicators.vwap import calculate_vwap
    from indicators.ema import calculate_ema
    from indicators.adx import calculate_adx
    from indicators.atr import calculate_atr
    from indicators.obv import calculate_obv

    if check_rsi:
        rsi_look_back = int(ta_settings["RSI"]['other_columns']['Lookback'])
        rsi_threshold = float(ta_settings["RSI"]['value'])
        if rsi_look_back >= len(df_TF):
            logger.warning(f"🚫 RSI lookback ({rsi_look_back}) >= DataFrame rows ({len(df_TF)}). Not enough data! ❌")
            return False
        rsi = calculate_rsi(df_TF['close'], rsi_look_back)
        rsi_last_value = rsi.iloc[-1]
        logger.info(f"📈 RSI last value {rsi_last_value}")
        if not rsi_last_value > rsi_threshold:
            logger.info(f"⚠️ RSI value {rsi_last_value} is not above threshold {rsi_threshold}, skipping signal ❌")
            return False

    if check_macd:
        macd_settings = ta_settings["MACD"]['parsed_value']
        macd_max_look_back = max(macd_settings)
        if macd_max_look_back >= len(df_TF):
            logger.warning(f"🚫 MACD lookback ({macd_max_look_back}) >= DataFrame rows ({len(df_TF)}). Not enough data! ❌")
            return False
        macd_line, signal_line, hist = calculate_macd(df_TF['close'], macd_settings[0], macd_settings[1], macd_settings[2])
        hist_last_value = hist.iloc[-1]
        logger.info(f"📊 MACD histogram last value {hist_last_value}")
        if not hist_last_value > 0:
            logger.info("⚠️ MACD histogram is not positive (<=0), skipping signal ❌")
            return False

    if check_vwap:
        vwap_threshold = ta_settings["VWAP"]['parsed_value']
        vwap_look_back = int(ta_settings['VWAP']['other_columns']['Lookback'])
        if vwap_look_back >= len(df_TF):
            logger.warning(f"🚫 VWAP lookback ({vwap_look_back}) >= DataFrame rows ({len(df_TF)}). Not enough data! ❌")
            return False
        vwap = calculate_vwap(df_TF)
        vwap_last_value = vwap.iloc[-1]
        vwap_filter = vwap_last_value * (1 + (vwap_threshold / 100))
        last_close = df_TF['close'].iloc[-1]
        logger.info(f"📊 Last close: {last_close}, VWAP filter level: {vwap_filter}")
        if not last_close > vwap_filter:
            logger.info(f"⚠️ Close {last_close} not greater than VWAP filter {vwap_filter}, skipping signal ❌")
            return False

    if check_ema:
        ema_settings = ta_settings["EMA"]['parsed_value']
        ema_max_look_back = max(ema_settings)
        if ema_max_look_back >= len(df_TF):
            logger.warning(f"🚫 EMA lookback ({ema_max_look_back}) >= DataFrame rows ({len(df_TF)}). Not enough data! ❌")
            return False
        ema1 = calculate_ema(df_TF['close'], ema_settings[0])
        ema2 = calculate_ema(df_TF['close'], ema_settings[1])
        ema1_last = ema1.iloc[-1]
        ema2_last = ema2.iloc[-1]
        logger.info(f"📊 EMA1 last: {ema1_last}, EMA2 last: {ema2_last}")
        if not ema1_last > ema2_last:
            logger.info(f"⚠️ EMA1 {ema1_last} not greater than EMA2 {ema2_last}, skipping signal ❌")
            return False

    if check_adx:
        adx_look_back = int(ta_settings["ADX"]['other_columns']['Lookback'])
        adx_threshold = float(ta_settings["ADX"]['value'])
        if adx_look_back >= len(df_TF):
            logger.warning(f"🚫 ADX lookback ({adx_look_back}) >= DataFrame rows ({len(df_TF)}). Not enough data! ❌")
            return False
        adx = calculate_adx(df_TF, adx_look_back)
        adx_last = adx.iloc[-1]
        logger.info(f"📈 ADX last value: {adx_last}")
        if not adx_last > adx_threshold:
            logger.info(f"⚠️ ADX {adx_last} not above threshold {adx_threshold}, skipping signal ❌")
            return False

    if check_atr:
        atr_settings = ta_settings["ATR"]['parsed_value']
        atr_max_look_back = max(atr_settings)
        if atr_max_look_back >= len(df_TF):
            logger.warning(f"🚫 ATR lookback ({atr_max_look_back}) >= DataFrame rows ({len(df_TF)}). Not enough data! ❌")
            return False
        atr1 = calculate_atr(df_TF, atr_settings[0])
        atr2 = calculate_atr(df_TF, atr_settings[1])
        atr1_last = atr1.iloc[-1]
        atr2_last = atr2.iloc[-1]
        logger.info(f"📊 ATR1 last: {atr1_last}, ATR2 last: {atr2_last}")
        if not atr1_last > atr2_last:
            logger.info(f"⚠️ ATR1 {atr1_last} not greater than ATR2 {atr2_last}, skipping signal ❌")
            return False

    if check_obv:
        obv_settings = int(ta_settings["OBV"]['parsed_value'])
        if len(df_TF) < 6:
            logger.warning(f"🚫 OBV requires minimum 6 rows, got {len(df_TF)} ❌")
            return False
        obv = calculate_obv(df_TF)
        obv_rising = ((obv.iloc[-1] - obv.iloc[-obv_settings-1]) / 5) > 0
        logger.info(f"📈 OBV rising: {obv_rising}")
        if not obv_rising:
            logger.info(f"⚠️ OBV not rising over last {obv_settings} periods, skipping signal ❌")
            return False
    
    logger.info("✅📈 All TA conditions are met...")
    return True