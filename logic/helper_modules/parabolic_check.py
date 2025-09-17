# file: logic/helpers/parabolic.py
"""
Parabolic gain filter.

Flags sequences where both percentage gain and body dominance are elevated,
indicating parabolic candles that should be filtered out.
"""

import numpy as np


def _body(opens, closes):
    """
    Absolute candle body per bar.
    """
    return np.abs(closes - opens)


def _total_range(highs, lows):
    """
    Total high-low range per bar.
    """
    return highs - lows


def passes_parabolic_gain(opens, highs, lows, closes, logger, gain_thresh=8.0, body_ratio_thresh=0.8):
    """
    Detect and reject parabolic candles.

    Criteria:
        - gains = (close - open) / open * 100 > gain_thresh
        - body_ratio = body / (high - low) > body_ratio_thresh
      If both conditions are true for any bar, the check fails.

    Parameters:
        opens, highs, lows, closes (pd.Series): OHLC series.
        logger: Logger for messages.
        gain_thresh (float): Minimum percent gain to be considered parabolic.
        body_ratio_thresh (float): Minimum body/range ratio indicating dominance.

    Returns:
        bool: True if no parabolic candles are present, else False.
    """
    bd = _body(opens, closes)
    rng = _total_range(highs, lows)

    # Use small epsilon in denominators to maintain numerical stability.
    gains = (closes - opens) * 100.0 / (opens + 1e-8)
    body_ratio = bd / (rng + 1e-8)

    if ((gains > gain_thresh) & (body_ratio > body_ratio_thresh)).any():
        logger.info("❌ MTF Parabolic candles present")
        return False
    return True
