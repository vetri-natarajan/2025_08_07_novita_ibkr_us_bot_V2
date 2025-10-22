"""
strategies/scalping_strategy.py
Orchestrator module that imports logic modules and exposes the main entry points:
- check_HTF_conditions
- check_MTF_conditions
- check_LTF_conditions
- check_technical_confluence
"""
# HTF and MTF checks
from logic.trend_filter import check_HTF_conditions as htf_check
from logic.volatility_filter import check_MTF_conditions as mtf_check

# Helper function imports

from logic.helper_modules import(
    price_breakout_confirm, 
    volume_confirmation,
    pullback_retest,
    check_technical_confluence    
    )

EPS = 1e-8


def check_HTF_conditions(symbol_combined, symbol, main_settings, ta_settings, max_look_back, df_HTF, logger):
    return htf_check(symbol_combined, symbol, main_settings, ta_settings, max_look_back,  df_HTF, logger)


def check_MTF_conditions(symbol_combined, symbol, main_settings, ta_settings, max_look_back, df_MTF, logger):
    return mtf_check(symbol_combined, symbol, main_settings, ta_settings, max_look_back, df_MTF, logger)


def check_LTF_conditions(symbol_combined, symbol, main_settings, ta_settings, max_look_back, df_LTF, df_HTF, logger, order_testing = False, is_live = False, live_price = None):
   
    if not order_testing:
        if df_LTF is None:
            logger.info("❌ LTF : Dataframe is None or too short")
            return False
    
        entry_decision = main_settings[symbol_combined]['Entry Decision'].upper()
        if entry_decision not in ["BREAKOUT", "PULLBACK", "BOTH"]:
            logger.info("⚠️ Entry decision input not correct")
            raise ValueError("Entry decision must be 'BREAKOUT', 'PULLBACK', or 'BOTH'")
    
        HH_LL_bars = int(main_settings[symbol_combined]['HHLL'])
        lastNHTF = df_HTF.iloc[-HH_LL_bars-1:-1].copy()
        #logger.info(f'LTF tail====>\n {df_LTF.tail()}')
        breakout_level = lastNHTF['high'].max()
        last = df_LTF.iloc[-1]
        last_close = last['close']
        last_vol = last['volume']
    
        if entry_decision in ["BREAKOUT", "BOTH"]:
            
            if not price_breakout_confirm(df_LTF, breakout_level, logger, is_live, live_price):
                return False
    
            vol_confirm_input = main_settings[symbol_combined]['Volume Confirm']
            htf_tf =  main_settings[symbol_combined]['Parsed TF'][0]
            if not volume_confirmation(df_LTF, df_HTF, vol_confirm_input, htf_tf, logger):
                return False
        
        if entry_decision in ["PULLBACK", "BOTH"]:
            if not pullback_retest(df_LTF, breakout_level, logger, is_live, live_price):
                return False
        
        '''
        # Technical confluence for LTF
        ltf_timeframe = main_settings[symbol_combined]["Parsed Raw TF"][2]
        if not check_technical_confluence(ltf_timeframe, df_LTF, ta_settings, main_settings, logger):
            logger.info("⚠️❌ LTF technical confluence not met")
            return False
        '''

        
    logger.info(f"✅📈 LTF [{symbol_combined}] all conditions are met...")
    
    return True


def check_TA_confluence(symbol, ALWAYS_TFS, data_cache, ta_settings, main_settings, logger):
    '''
    1. we need to use the check_technicla_Confluence module
    2 pass timeframe anddfs
    3. for time frames take from ta_setttings
    4 get the df_TF form steremin data
    
    '''
    TF_TO_TIMEFRAME = {
        '1 week': '1W',
        '1 day': '1D',
        '4 hours': '4H',
        '1 hour': '1H',
        '30 mins': '30m',
        '15 mins': '15m',
        '5 mins': '5m',
        '1 min': '1m'
        
        }
    
    for timeframe in ALWAYS_TFS:
        #get the dfs
        timeframe_ = TF_TO_TIMEFRAME[timeframe]
        df_TFs = data_cache.get(symbol)
        df_TF = df_TFs[timeframe][:-1]
       
        if not check_technical_confluence(timeframe_, df_TF, ta_settings, main_settings, logger):
            logger.info(f"⚠️❌ TA Confluence [{symbol}] {timeframe} confluence not met")
            import time
            #time.sleep(15)
                
            return False  
        
        else: 
            #import time
            #time.sleep(15)
            #if len(count) > 0:
            logger.info(f"✅ TA Confluence [{symbol}] {timeframe} confluence met")
    

        
    
    return True