# file: logic/mtf_conditions.py
"""
Public entry for MTF condition checks.

This module orchestrates the multi-timeframe (MTF) checks by delegating
discrete responsibilities to helper modules under logic.helpers.

Exports:
- check_MTF_conditions: Main function to validate MTF conditions against settings and data.
"""

from logic.helper_modules import (
    resolve_mtf_lookback,
    extract_ohlc_as_float,
    passes_volatility_filter,
    passes_upper_wick_body_ratio,
    passes_upper_wick_total_range_ratio,
    passes_parabolic_gain,
    check_technical_confluence
)


def check_MTF_conditions(symbol_combined, symbol, main_settings, ta_settings, max_look_back, df_MTF, logger):
    """
    Validate that the given MTF dataframe meets a series of filters.

    Parameters:
        symbol (str): Instrument key used to access per-symbol settings.
        main_settings (dict): Configuration dict holding thresholds and TF info.
        ta_settings (dict): Technical analysis-specific configuration passed through.
        max_look_back (int): Unused externally provided parameter (kept for compatibility).
        df_MTF (pd.DataFrame): DataFrame containing OHLC columns for the MTF.
        logger: Logger with .info method for diagnostics.

    Returns:
        bool: True if all checks pass, otherwise False.

    Raises:
        ValueError: If the higher and medium time frames are mismatched per settings.
    """
    # Defensive check for data presence.
    if df_MTF is None:
        logger.info("❌ MTF : Dataframe is None or too short")
        return False

    # Compute how many recent bars to consider based on TF pairing rules.
    mtf_look_back = resolve_mtf_lookback(symbol_combined, symbol, main_settings, logger)

    # Slice last N bars, excluding the most recent incomplete one by convention.
    logger.info(f"📝 MTF before lastN ====>\n {df_MTF}")
    lastN = df_MTF.iloc[-(mtf_look_back + 1): -1].copy()
    logger.info(f"MTF lastN====>\n {lastN}")

    # Extract typed OHLC series for vectorized computations.
    opens, highs, lows, closes = extract_ohlc_as_float(lastN)

    # 1) Volatility filter (per-bar percent range).
    if not passes_volatility_filter(symbol_combined, symbol, main_settings, highs, lows, logger):
        return False

    # 2) Upper wick to body ratio cap.
    if not passes_upper_wick_body_ratio(opens, highs, closes, logger):
        return False

    # 3) Upper wick to total range ratio cap.
    if not passes_upper_wick_total_range_ratio(opens, highs, lows, closes, logger):
        return False

    # 4) Parabolic gain/body ratio filter.
    if not passes_parabolic_gain(opens, highs, lows, closes, logger):
        return False

    # 5) Technical confluence check at the MTF timeframe.
    mtf_timeframe = main_settings[symbol_combined]["Parsed Raw TF"][1]
    if not check_technical_confluence(mtf_timeframe, df_MTF, ta_settings, main_settings, logger):
        logger.info(f"⚠️❌ MTF technical confluence for {symbol_combined} not met...")
        return False

    logger.info(f"✅📈 MTF for {symbol_combined} all conditions are met...")
    return True
