def price_breakout_confirm(df_LTF, breakout_level, logger, is_live = False, live_price = None):
    if not is_live:  #for backtesting
        current_price = df_LTF['high'].iloc[-1] 
    else:
        current_price = live_price
          
    if current_price is None:
        logger.info("❌last price is none")
    if breakout_level is None:
        logger.info("❌breakout_level is none")   
        
    price_breakout = current_price > breakout_level
    if not price_breakout:
        logger.info(f"❌ LTF no breakout current_price: {current_price} breakout_level {breakout_level}")
        return False
    return True
