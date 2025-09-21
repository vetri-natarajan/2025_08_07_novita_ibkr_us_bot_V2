import pandas as pd

def volume_confirmation(df_LTF, df_HTF, vol_confirm_input, logger):
    last_vol = df_LTF['volume'].iloc[-1]
    last_5_vol = df_LTF['volume'].iloc[-6:-1]
    last_5_vol_avg = last_5_vol.mean()

    vol_confirm_input = vol_confirm_input.upper()
    if vol_confirm_input in ['L', "LOW"]:
        vol_confirmation = last_vol > 2.5 * last_5_vol_avg
        if not vol_confirmation:
            logger.info(f"❌ LTF No Volume confirmation last_volume: {last_vol} breakout_volume {2.5 * last_5_vol_avg}")
            return False

    elif vol_confirm_input in ['M', "MODERATE"]:
        from indicators.vwap import calculate_vwap
        vwap = calculate_vwap(df_LTF)
        last_vwap = vwap.iloc[-1]
        vol_confirmation = last_vol > last_vwap
        if not vol_confirmation:
            logger.info(f"❌ LTF No Volume confirmation last_volume: {last_vol} vwap_volume {last_vwap}")
            return False

    elif vol_confirm_input in ['H', "HIGH"]:
        logger.info(f"tail htf ===> {df_HTF}")
        last_htf_bar_time = df_HTF.index[-1]
        htf_bar_open_time = last_htf_bar_time - pd.Timedelta(minutes=30)

        tz = df_LTF.index.tz
        if last_htf_bar_time.tzinfo is None:
            last_htf_bar_time = last_htf_bar_time.tz_localize(tz)
        if htf_bar_open_time.tzinfo is None:
            htf_bar_open_time = htf_bar_open_time.tz_localize(tz)

        ltf_bars_in_htf = df_LTF[(df_LTF.index > htf_bar_open_time) & (df_LTF.index <= last_htf_bar_time)]

        highest_ltf_volume = ltf_bars_in_htf['volume'].max()
        current_volume = df_LTF['volume'].iloc[-1]
        vol_confirmation = current_volume > highest_ltf_volume

        if not vol_confirmation:
            logger.info(f"❌ LTF No Volume confirmation last_volume: {current_volume} highest_ltf_volume: {highest_ltf_volume}")
            return False

    else:
        logger.info("⚠️ Vol_confirm_input not recognized")
        return False

    return True
