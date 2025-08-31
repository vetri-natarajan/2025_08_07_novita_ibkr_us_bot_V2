"""
indicators/rsi.py

Purpose:
- Exponential-weighted RSI implementation.
"""
import pandas as pd
import numpy as np

def calculate_rsi(series: pd.Series, n: int = 14) -> pd.Series:
    delta = series.diff()
    up = delta.clip(lower=0)
    down = -1 * delta.clip(upper=0)
    ma_up = up.ewm(com=(n - 1), adjust=False).mean()
    ma_down = down.ewm(com=(n - 1), adjust=False).mean()
    rs = ma_up / (ma_down + 1e-8)
    return 100 - (100 / (1 + rs))
