import numpy as np

def price_breakout_confirm(df_LTF, breakout_level, logger, is_live = False, live_price = None):
    if df_LTF is None or len(df_LTF) < 1:
        logger.info("❌ LTF : Dataframe is None or too short")
        return False
    if not is_live:
        last_close = df_LTF['close'].iloc[-1]
    else:
        last_close = live_price
    if last_close is None:
        logger.info("❌last close is none")
    if breakout_level is None:
        logger.info("❌breakout_levele is none")   
        
    price_breakout = last_close > breakout_level
    if not price_breakout:
        logger.info(f"❌ LTF no breakout last_price: {last_close} breakout_level {breakout_level}")
        return False
    return True
