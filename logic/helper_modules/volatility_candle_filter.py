# file: logic/helpers/volatility.py
"""
Volatility filter.

Checks that per-bar percentage range stays within configured bounds.
"""

import numpy as np


def passes_volatility_filter(symbol_combined, symbol, main_settings, closes, logger):
    """
    Validate per-bar volatility is within configured [lower, upper) bounds.

    Volatility is defined as: (high - low) / low * 100.
    A small epsilon and zero-safe denominator guard are used.

    Parameters:
        symbol (str): Instrument key for per-symbol volatility settings.
        main_settings (dict): Contains 'Parsed Each Volatility' [lower, upper].
        highs, lows (pd.Series): High and Low series (float).
        logger: Logger for diagnostics.

    Returns:
        bool: True if all bars pass the configured bounds, else False.
    """
    # Avoid divide-by-zero; replace 0 lows with NaN then add epsilon to denominator.
    #vol_percent = (highs - lows) * 100.0 / (lows.replace(0, np.nan) + 1e-8)
    vol_percent = (closes - closes.shift(-1)) * 100.0 / (closes.replace(0, np.nan) + 1e-8)
    
    vol_filters = main_settings[symbol_combined]['Parsed Each Volatility']
    vol_filter_lower = vol_filters[0]
    vol_filter_upper = vol_filters[1]

    # Strictly within (lower, upper), mirroring original inequalities.
    within = (vol_percent > vol_filter_lower) & (vol_percent < vol_filter_upper)
    if not np.all(within.values):
        logger.info(
            f"❌ MTF Volatility is not within the range: {vol_filter_lower} to {vol_filter_upper}"
        )
        return False
    return True
