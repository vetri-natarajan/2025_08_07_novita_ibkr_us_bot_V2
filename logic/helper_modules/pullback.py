import numpy as np

def pullback_retest(df_LTF, breakout_level, logger,  is_live = False, live_price = None):
    
    if df_LTF.empty or len(df_LTF) < 6:
        logger.error("❌ Insufficient LTF data length")
        return False
    
    retest_lower = breakout_level * (1 - 0.003)  # -0.3%
    retest_upper = breakout_level * (1 + 0.003)  # +0.3%
    last_5_df = df_LTF[-6:-1]
    max_high5 = np.max(last_5_df['high'])
    min_low5 = np.min(last_5_df['low'])
    within_retest = (max_high5 < retest_upper) and (min_low5 < retest_lower)

    logger.info(f"📈 LTF retest upper: {retest_upper} 🔝 max high: {max_high5} 📉 lower: {retest_lower} 🔽 min low: {min_low5}")
    if not within_retest:
        logger.info("❌ LTF Price not within retest bounds")
        return False

    last_5_vol = df_LTF[-6:-1]['volume']
    
    crossed_df = df_LTF[df_LTF['high'] > breakout_level]

    if not crossed_df.empty:
        breakout_vol = crossed_df['volume'].iloc[0]
        valid_pullback = (last_5_vol / breakout_vol).any() < 0.8

        if not valid_pullback:
            logger.info(f"❌ LTF Pullback retest volume not passed: breakout_vol {breakout_vol}")
            return False
    else:
        logger.info("❌ LTF Pullback retest volume not passed: crossed_df is empty")
        return False

    previous_candle_open = df_LTF['open'].iloc[-2]
    previous_candle_close = df_LTF['close'].iloc[-2]    
        
    if previous_candle_open > previous_candle_close:
        logger.info(f"❌ LTF Pullback last close {previous_candle_close} lower than last open {previous_candle_open}")
        return False
    
    if not is_live:
        current_price = df_LTF['high'].iloc[-1]
    else:
        current_price = live_price

    if not current_price > breakout_level:
        logger.info(f"❌ LTF current_price {current_price} lower than breakout level {breakout_level}")
        return False

    return True
