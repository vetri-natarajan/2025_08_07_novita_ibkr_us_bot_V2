# -*- coding: utf-8 -*-
"""
strategies/scalping_strategy

It checks for 
1. HTF trend filter
2. voaltitly filter
3.1 min breakout confirmation
"""
import pandas as pd
import numpy as np

from indicators.rsi import calculate_rsi
from indicators.macd import calculate_macd
from indicators.vwap import calculate_vwap
from indicators.ema import calculate_ema
from indicators.adx import calculate_adx
from indicators.atr import calculate_atr
from indicators.obv import calculate_obv

EPS = 1e-8

def check_technical_confluence(timeframe, df_TF, ta_settings, main_settings, logger):
    #<========================================================================>
    #TA settings 
    
    yes_list = ["Y", "YES", "TRUE", "1"]
    
    check_rsi = ta_settings["RSI"]["other_columns"][timeframe].upper() in yes_list
    check_macd = ta_settings["MACD"]["other_columns"][timeframe].upper() in yes_list
    check_vwap = ta_settings["VWAP"]["other_columns"][timeframe].upper() in yes_list
    check_ema= ta_settings["EMA"]["other_columns"][timeframe].upper() in yes_list
    check_adx = ta_settings["ADX"]["other_columns"][timeframe].upper() in yes_list
    check_atr = ta_settings["ATR"]["other_columns"][timeframe].upper() in yes_list
    check_obv = ta_settings["OBV"]["other_columns"][timeframe].upper() in yes_list
    

        
    if check_rsi: 
        rsi_look_back = int(ta_settings["RSI"]['other_columns']['Lookback'])
        rsi_threshold = float(ta_settings["RSI"]['value'])
        if rsi_look_back >= len(df_TF):
            logger.warning(f"🚫 RSI lookback ({rsi_look_back}) >= DataFrame rows ({len(df_TF)}). Not enough data! ❌")
            return False
        df_rsi = df_TF
        rsi = calculate_rsi(df_rsi['close'], rsi_look_back)
        rsi_last_value = rsi.iloc[-1]
        logger.info(f"📈 RSI last value {rsi_last_value}")
        if not rsi_last_value > rsi_threshold:
            logger.info(f"⚠️ RSI value {rsi_last_value} is not above the threshold {rsi_threshold}, skipping this signal ❌")
            return False
    
    if check_macd:
        macd_settings = ta_settings["MACD"]['parsed_value']
        macd_max_look_back = max(macd_settings)
        if macd_max_look_back >= len(df_TF):
            logger.warning(f"🚫 MACD lookback ({macd_max_look_back}) >= DataFrame rows ({len(df_TF)}). Not enough data! ❌")
            return False
        df_macd = df_TF
        macd_line, signal_line, hist = calculate_macd(df_macd['close'], 
                                                  macd_settings[0], 
                                                  macd_settings[1], 
                                                  macd_settings[2])
        hist_last_value = hist.iloc[-1]
        logger.info(f"📊 MACD histogram last value {hist_last_value}")   
        if not hist_last_value > 0:
            logger.info("⚠️ MACD histogram is not positive (<= 0), skipping this signal ❌")
            return False
    
    if check_vwap:
        vwap_threshold = ta_settings["VWAP"]['parsed_value']
        vwap_look_back = int(ta_settings['VWAP']['other_columns']['Lookback'])
        if vwap_look_back >= len(df_TF):
            logger.warning(f"🚫 VWAP lookback ({vwap_look_back}) >= DataFrame rows ({len(df_TF)}). Not enough data! ❌")
            return False
        df_vwap = df_TF
        vwap = calculate_vwap(df_vwap)
        vwap_last_value = vwap.iloc[-1]
        logger.info(f"📊 VWAP last value {vwap_last_value}")    
        if not vwap_last_value > vwap_threshold:
            logger.info(f"⚠️ VWAP {vwap_last_value} is not greater than threshold {vwap_threshold}, skipping this signal ❌")
            return False
    
    if check_ema:
        ema_settings = ta_settings["EMA"]['parsed_value']
        ema_max_look_back = max(ema_settings)
        if ema_max_look_back >= len(df_TF):
            logger.warning(f"🚫 EMA lookback ({ema_max_look_back}) >= DataFrame rows ({len(df_TF)}). Not enough data! ❌")
            return False
        df_ema = df_TF
        ema1 = calculate_ema(df_ema['close'], ema_settings[0])
        ema2 = calculate_ema(df_ema['close'], ema_settings[1])
        ema1_last_value = ema1.iloc[-1]
        ema2_last_value = ema2.iloc[-1]
        logger.info(f"📊 EMA1 last value: {ema1_last_value} EMA2 last value: {ema2_last_value}")    
        if not ema1_last_value > ema2_last_value:
            logger.info(f"⚠️ EMA1 last value: {ema1_last_value} not greater than EMA2 last value: {ema2_last_value}, skipping this signal ❌")
            return False
    
    if check_adx:
        adx_look_back = int(ta_settings["ADX"]['other_columns']['Lookback'])
        adx_threshold = float(ta_settings["ADX"]['value'])
        if adx_look_back >= len(df_TF):
            logger.warning(f"🚫 ADX lookback ({adx_look_back}) >= DataFrame rows ({len(df_TF)}). Not enough data! ❌")
            return False
        df_adx = df_TF
        adx = calculate_adx(df_adx, adx_look_back)
        adx_last_value = adx.iloc[-1]
        logger.info(f"📈 ADX last value {adx_last_value}")
        if not adx_last_value > adx_threshold:
            logger.info(f"⚠️ ADX value {adx_last_value} is not above the threshold {adx_threshold}, skipping this signal ❌")
            return False
    
    if check_atr:
        atr_settings = ta_settings["ATR"]['parsed_value']
        atr_max_look_back = max(atr_settings)
        if atr_max_look_back >= len(df_TF):
            logger.warning(f"🚫 ATR lookback ({atr_max_look_back}) >= DataFrame rows ({len(df_TF)}). Not enough data! ❌")
            return False
        df_atr = df_TF
        atr1 = calculate_atr(df_atr, atr_settings[0])
        atr2 = calculate_atr(df_atr, atr_settings[1])
        atr1_last_value = atr1.iloc[-1]
        atr2_last_value = atr2.iloc[-1]
        logger.info(f"📊 ATR1 last value: {atr1_last_value} ATR2 last value: {atr2_last_value}")    
        if not atr1_last_value > atr2_last_value:
            logger.info(f"⚠️ ATR last value: {atr1_last_value} not greater than ATR2 last value: {atr2_last_value}, skipping this signal ❌")
            return False      
    
    if check_obv:
        obv_settings = int(ta_settings["OBV"]['parsed_value'])
        if len(df_TF) < 6:
            logger.warning(f"🚫 OBV requires minimum 6 rows, but only {len(df_TF)} available ❌")
            return False
        df_obv = df_TF
        obv = calculate_obv(df_obv)
        obv_rising = ((obv.iloc[-1] - obv.iloc[-obv_settings -1]) / 5) > 0
        logger.info(f"📈 OBV rising: {obv_rising}")
        if not obv_rising:
            logger.info(f"⚠️ OBV not rising over the last {obv_settings} periods, skipping this signal ❌")
            return False

            
    
    return True

def check_HTF_conditions(symbol, main_settings, ta_settings, df_HTF: pd.DataFrame, logger) -> bool:
    
    HH_LL_bars = int(main_settings[symbol]['HHLL'])
    each_gain_limits = main_settings[symbol]['Parsed Each Gain']
    total_gain_limits = main_settings[symbol]['Parsed Total Gain']
    dd_limits = float(main_settings[symbol]['Max Draw down'].replace("%", "").strip())
    

    
    #<========================================================================>
    # Empty checks
    if df_HTF is None or len(df_HTF) < HH_LL_bars:
        logger.info("❌ HTF: Dataframe is None or too short")
        return False
    #print(df_HTF)
    lastN = df_HTF.iloc[-HH_LL_bars-1:-1].copy()
   # print(type(lastN))
    opens = lastN['open'].astype(float)
    highs = lastN['high'].astype(float)
    lows = lastN['low'].astype(float)
    closes = lastN['close'].astype(float)
    
    #<========================================================================>
    # 1. Green closes 
    if not all(closes > opens):
        logger.info("❌ HTF: Not all closes > opens")
        return False

    #<========================================================================>
    # 2. Individual Gains
    gains = (closes - opens)/(opens + EPS)
    
    if not all((gains > each_gain_limits[0]) & (gains < each_gain_limits[1])):
        logger.info(f"❌ HTF: Gains not in range {each_gain_limits[0]} to {each_gain_limits[1]}")
        return False

    #<========================================================================>
    # 3. Total Gains
    
    if not total_gain_limits[0] < gains.sum() < total_gain_limits[1]:
        logger.info(f"❌ HTF: Toatl Gains {each_gain_limits[0]} condition not satisfied")
        return False
        
     
    #<========================================================================>
    # 4. Drawdowns  
    peak = highs.max()
    trough = lows.min()
    drawdown = (peak - trough)*100/(peak + EPS)
    if drawdown > dd_limits:
        logger.info(f"❌ HTF: Drawdown {drawdown:.4f} >= {dd_limits}")
        return False

    #<=================Technical confluence===================================>
    
    htf_timeframe = main_settings[symbol]["Parsed Raw TF"][0]
    technical_confluence = check_technical_confluence(htf_timeframe, df_HTF, ta_settings, main_settings, logger)  
    if not technical_confluence:
        logger.info("⚠️❌ HTF technical confluence not met...")
        return False
    return True       
        
def check_MTF_conditions(symbol, main_settings, ta_settings,  df_MTF: pd.DataFrame, logger) -> bool:
    #print("==================================================================")
    #print(ta_settings)
    if df_MTF is None:
        logger.info("❌ MTF : Dataframe is None or too short")
        return False
    HH_LL_bars = int(main_settings[symbol]['HHLL'])
    parsed_HTF = main_settings[symbol]["Parsed TF"][0]
    parsed_MTF = main_settings[symbol]["Parsed TF"][1]
    
    mtf_look_back = 0
    if parsed_HTF == '30 mins' and parsed_MTF == '5 mins':
        mtf_look_back = HH_LL_bars*6;
        
    elif parsed_HTF == '1 week' and parsed_MTF == '1 day':
         mtf_look_back = HH_LL_bars*5;       
 
    else:
        logger.info("❌ MTF Higher and medium time frame mismatch detected 🚩")
        raise ValueError("❌ MTF Higher and medium time frame mismatch detected 🚩")
        
    lastN = df_MTF.iloc[-(mtf_look_back + 1): -1].copy()
    opens = lastN['open'].astype(float)
    closes = lastN['close'].astype(float)
    highs = lastN['high'].astype(float)
    lows = lastN['low'].astype(float)
    
    
    #<========================================================================>
    # 1. volatility check
    vol_percent = (highs - lows)*100/lows
    vol_filters = main_settings[symbol]['Parsed Each Volatility']
    vol_filter_lower = vol_filters[0]
    vol_filter_upper = vol_filters[1]
    
    if not all((vol_filter_lower < vol_percent) & (vol_filter_upper > vol_percent)):
        logger.info(f"❌ MTF Volatility is not within the range: {vol_filter_lower} to {vol_filter_upper}")
        return False
    
        
    #<========================================================================>
    # 2. wick body ratio check
    upper_wick = highs - np.maximum(opens, closes)
    body = np.abs(closes - opens)
    upper_wick_body_ratio = upper_wick / (body + EPS)
    if (upper_wick_body_ratio > 2.5).any():
        logger.info("❌ MTF upper_wick_body_ratio is greater than 2.5")
        return False    

    #<========================================================================>
    # 3. wick  ratio check 
    total_range = highs - lows
    upper_wick_ratio = upper_wick/(total_range + EPS)
    
    if (upper_wick_ratio > 0.6).any():
        logger.info("❌ MTF upper_wick_ratio is greater than 0.6")
        return False        
      
    #<========================================================================>
    # 4. parabolic gain check  
    gains = (closes - opens)*100/(opens + EPS)
    body_ratio = body/(total_range + EPS)
    if ((gains > 8) & body_ratio > 0.8).any() :
        logger.info("❌ MTF Parabloic candles present") 
        return False
        
        
    #<=================Technical confluence===================================>
    
    mtf_timeframe = main_settings[symbol]["Parsed Raw TF"][1]
    technical_confluence = check_technical_confluence(mtf_timeframe, df_MTF, ta_settings, main_settings, logger)
    if not technical_confluence:
        logger.info("⚠️❌ HTF technical confluence not met...")
        return False
    return True    

def check_LTF_conditions(symbol, main_settings, ta_settings, df_LTF: pd.DataFrame, df_HTF: pd.DataFrame, logger) -> bool:
    if df_LTF is None:
        logger.info("❌ LTF : Dataframe is None or too short")
        return False

    
    entry_decision = (main_settings[symbol]['Entry Decision']).upper() 
    
    if entry_decision not in ["BREAKOUT", "PULLBACK", "BOTH"]:
        logger.info("⚠️ Entry decision input not correct... please check ❌")
        raise ValueError("⚠️ Entry decision must be 'BREAKOUT' or 'PULLBACK'")
    
    HH_LL_bars = int(main_settings[symbol]['HHLL'])
    lastNHTF = df_HTF.iloc[-HH_LL_bars-1:-1].copy()
    #print(lastNHTF)
    breakout_level = np.max(lastNHTF['high'])   
    last = df_LTF.iloc[-1]
    last_close = last['close']
    last_vol = last['volume']
    price = last['close']
    
    if entry_decision in ["BREAKOUT", "BOTH"]:        
        
        #<========================================================================>
        # 1. price breakout

        
        price_breakout = last_close > breakout_level       
        
        if not price_breakout:
            logger.info(f"❌ LTF no breakout last_price: {last_close} breakout_level {breakout_level}")
            return False    
        
        #<========================================================================>
        # 2. Volume confirmation
        last_5_vol =  df_LTF['volume'].iloc[-6:-1]
        last_5_vol_avg = np.mean(last_5_vol)
        vol_confirm_input = main_settings[symbol]['Volume Confirm'].upper()
        
        if not vol_confirm_input in ['H', 'L', 'M', 'HIGH', 'LOW', 'MODERATE']:
            logger.info("⚠️ Vol_confirm_input not correct... please check ❌")
            raise ValueError("⚠️ Vol_confirm_input must be one of the following: 'H', 'L', 'M', 'HIGH', 'LOW', 'MODERATE'")
                 
        
        if vol_confirm_input in ['L', "LOW"]:
            vol_confirmation = last_vol > 2.5*last_5_vol_avg
            
            if not vol_confirmation:
                logger.info(f"❌ LTF No Volume confiramtion  last_volume: {last_vol} breakout_volume {2.5*last_5_vol_avg}")
                return False              
            
        if vol_confirm_input in ['M', "MODERATE"]:
            vwap = calculate_vwap(df_LTF)
            last_vwap = vwap.iloc[-1]
            vol_confirmation = last_vol >last_vwap
            
            if not vol_confirmation:
                logger.info(f"❌ LTF No Volume confiramtion  last_volume: {last_vol} vwap_volume {last_vwap}")
                return False   
                    
        if vol_confirm_input in ['H', "HIGH"]:
                        
            # Get last closed HTF bar end time (timestamp)
            
            #print()
            #print(df_HTF.columns)
            last_htf_bar_time = df_HTF.index[-1]
            
            #print()
            #print(last_htf_bar_time)
            

            
            
            # Get the HTF bar open time by subtracting time delta (30 minutes)
            htf_bar_open_time = last_htf_bar_time - pd.Timedelta(minutes=30)
            
            
            tz = df_LTF.index.tz
            
            if last_htf_bar_time.tzinfo is None:
                last_htf_bar_time = last_htf_bar_time.tz_localize(tz)
            if htf_bar_open_time.tzinfo is None:
                htf_bar_open_time = htf_bar_open_time.tz_localize(tz)
            
            # Filter LTF bars that fall within this HTF bar time range
            ltf_bars_in_htf = df_LTF[(df_LTF.index > htf_bar_open_time) & (df_LTF.index <= last_htf_bar_time)]
            
            # Find highest volume in these filtered LTF bars
            highest_ltf_volume = ltf_bars_in_htf['volume'].max()
            
            #print("highest_ltf_volume", highest_ltf_volume)
            
            # Get current 1-min candle volume (last row of df_LTF)
            current_volume = df_LTF['volume'].iloc[-1]
            vol_confirmation = current_volume > highest_ltf_volume
            # Check condition
            
            if vol_confirmation:
                logger.info(f"❌ LTF No Volume confiramtion  last_volume: {current_volume} highest_ltf_volume: {highest_ltf_volume}")
                return False 
        
    if entry_decision in ["PULLBACK", "BOTH"]:
        #<========================================================================>
        # 1. retest confirmation
        retest_lower = breakout_level * (1 - 0.003)  # -0.3%
        retest_upper = breakout_level * (1 + 0.003)  # +0.3%
        last_5_df = df_LTF[-5:]
        max_high5 = np.max(last_5_df['high'])
        max_low5 = np.max(last_5_df['low'])
        within_retest = (max_high5 < retest_upper)  and (max_low5 < retest_lower)
        
        logger.info(f" LTF retest upper:{retest_upper} max high{max_high5} lower: {retest_lower} min low: {max_low5}")

        if not within_retest:
            logger.info(f"❌ LTF Price not within retest upper:{retest_upper} max high{max_high5} lower: {retest_lower} min low: {max_low5}")
            return False         
        
        
        if within_retest:            
            #<========================================================================>
            # 2. Pullback volume
            last_5_vol = df_LTF[-5:]['volume']
            crossed_df = df_LTF[df_LTF['close'] > breakout_level]
            
            #print('crossed_df===>')
            #print(crossed_df)
            
            if not crossed_df.empty:
                breakout_vol = crossed_df['volume'][0]
                valid_pullback = (last_5_vol/breakout_vol).any() < 0.8
                
                if not valid_pullback:
                    logger.info(f"❌ LTF Pullback retest volume not passed: breakout_vol {breakout_vol} ")
                    #return False 
            else: 
                logger.info(f"❌ LTF Pullback retest volume not passed: crossed_df is empty")
                #return False 
                
                          
                
            #<========================================================================>
            # 3. Last candle bullish
            
            last_open = df_LTF['open'][-1]
            last_close = df_LTF['close'][-1]
            if last_open < last_close:
                logger.info(f"❌ LTF Pullback last close {last_close} lower than last open {last_open} ")
                #return False   
            

            #<========================================================================>
            # 4. Last candle bullish
            last_high = df_LTF['high'][-1]
            if not last_high > breakout_level:
                logger.info(f"❌ LTF last high {last_high} lower than breakoout level {breakout_level} ")
                #return False           
        
    #<=================Technical confluence===================================>
    
    ltf_timeframe = main_settings[symbol]["Parsed Raw TF"][2]
    technical_confluence = check_technical_confluence(ltf_timeframe, df_LTF, ta_settings, main_settings, logger)
    
    if not technical_confluence:
        logger.info("⚠️❌ HTF technical confluence not met...")
        #return False
    return True    
                  
