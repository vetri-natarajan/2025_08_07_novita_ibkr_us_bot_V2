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
        logger.info(f'LTF tail====>\n {df_LTF.tail()}')
        breakout_level = lastNHTF['high'].max()
        last = df_LTF.iloc[-1]
        last_close = last['close']
        last_vol = last['volume']
    
        if entry_decision in ["BREAKOUT", "BOTH"]:
            
            if not price_breakout_confirm(df_LTF, breakout_level, logger, is_live, live_price):
                return False
    
            vol_confirm_input = main_settings[symbol_combined]['Volume Confirm']
            if not volume_confirmation(df_LTF, df_HTF, vol_confirm_input, logger):
                return False
        
        if entry_decision in ["PULLBACK", "BOTH"]:
            if not pullback_retest(df_LTF, breakout_level, logger):
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


def check_technical_confluence(timeframe, df_TF, ta_settings, main_settings, logger) -> bool:
    pass

def check_TA_confluence(ALWAYS_TFS, timeframe, df_TF, ta_settings, main_settings, logger):
    
    pass