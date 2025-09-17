# file: logic/helpers/wicks.py
"""
Wick/body/Range ratio checks.

Implements:
- Upper wick to body ratio ceiling.
- Upper wick to total range ratio ceiling.
"""

import numpy as np


def _upper_wick(opens, highs, closes):
    """
    Compute upper wick size per bar: high - max(open, close).
    """
    return highs - np.maximum(opens, closes)


def _body(opens, closes):
    """
    Compute absolute candle body per bar.
    """
    return np.abs(closes - opens)


def _total_range(highs, lows):
    """
    Compute total high-low range per bar.
    """
    return highs - lows


def passes_upper_wick_body_ratio(opens, highs, closes, logger, threshold=2.5):
    """
    Enforce an upper wick to body ratio ceiling.

    Ratio: upper_wick / body. Bars exceeding threshold cause failure.

    Parameters:
        opens, highs, closes (pd.Series): OHLC series.
        logger: Logger for context messages.
        threshold (float): Max allowed ratio (default 2.5).

    Returns:
        bool: True if all bars are within threshold, else False.
    """
    uw = _upper_wick(opens, highs, closes)
    bd = _body(opens, closes)

    # Add epsilon to avoid division by zero on doji-like bars.
    ratio = uw / (bd + 1e-8)
    if (ratio > threshold).any():
        logger.info(f"❌ MTF upper_wick_body_ratio is greater than {threshold}")
        return False
    return True


def passes_upper_wick_total_range_ratio(opens, highs, lows, closes, logger, threshold=0.6):
    """
    Enforce an upper wick to total range ratio ceiling.

    Ratio: upper_wick / (high - low). Bars exceeding threshold cause failure.

    Parameters:
        opens, highs, lows, closes (pd.Series): OHLC series.
        logger: Logger for context messages.
        threshold (float): Max allowed ratio (default 0.6).

    Returns:
        bool: True if all bars are within threshold, else False.
    """
    uw = _upper_wick(opens, highs, closes)
    rng = _total_range(highs, lows)

    # Add epsilon to avoid division by zero on flat bars.
    ratio = uw / (rng + 1e-8)
    if (ratio > threshold).any():
        logger.info(f"❌ MTF upper_wick_ratio is greater than {threshold}")
        return False
    return True
