# -*- coding: utf-8 -*-
"""
Created on Fri Aug 22 08:02:55 2025

@author: Vetriselvan
"""

import pandas as pd

def calculate_ema(series, span=14):
    """
    Calculates the Exponential Moving Average (EMA) for a Pandas Series.

    Args:
        series (pd.Series): Input data series (e.g., closing prices).
        span (int): The period for EMA calculation. Typical values are 10, 14, 21, etc.

    Returns:
        pd.Series: The EMA values as a Pandas Series.
    """
    ema = series.ewm(span=span, adjust=False).mean()
    return ema

