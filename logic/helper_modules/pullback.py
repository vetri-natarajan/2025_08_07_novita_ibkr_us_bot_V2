import numpy as np

def pullback_retest(df_LTF, breakout_level, logger):
    retest_lower = breakout_level * (1 - 0.003)  # -0.3%
    retest_upper = breakout_level * (1 + 0.003)  # +0.3%
    last_5_df = df_LTF[-5:]
    max_high5 = np.max(last_5_df['high'])
    max_low5 = np.max(last_5_df['low'])
    within_retest = (max_high5 < retest_upper) and (max_low5 < retest_lower)

    logger.info(f" LTF retest upper:{retest_upper} max high:{max_high5} lower:{retest_lower} max low:{max_low5}")
    if not within_retest:
        logger.info(f"❌ LTF Price not within retest bounds")
        return False

    last_5_vol = df_LTF[-5:]['volume']
    crossed_df = df_LTF[df_LTF['close'] > breakout_level]

    if not crossed_df.empty:
        breakout_vol = crossed_df['volume'].iloc[0]
        valid_pullback = (last_5_vol / breakout_vol).any() < 0.8

        if not valid_pullback:
            logger.info(f"❌ LTF Pullback retest volume not passed: breakout_vol {breakout_vol}")
            return False
    else:
        logger.info(f"❌ LTF Pullback retest volume not passed: crossed_df is empty")
        return False

    last_open = df_LTF['open'].iloc[-1]
    last_close = df_LTF['close'].iloc[-1]
    if last_open > last_close:
        logger.info(f"❌ LTF Pullback last close {last_close} lower than last open {last_open}")
        return False

    last_high = df_LTF['high'].iloc[-1]
    if not last_high > breakout_level:
        logger.info(f"❌ LTF last high {last_high} lower than breakout level {breakout_level}")
        return False

    return True
