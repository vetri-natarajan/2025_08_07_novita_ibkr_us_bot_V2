import pandas as pd
import numpy as np

def calculate_atr(df, period=14):
    """
    Calculates the Average True Range (ATR) for a DataFrame of OHLC data.

    Args:
        df (pd.DataFrame): DataFrame with columns 'high', 'low', 'close'.
        period (int): The lookback period for ATR calculation, default is 14.

    Returns:
        pd.Series: ATR values.
    """
    high = df['high']
    low = df['low']
    close = df['close']

    # Calculate True Range (TR)
    prev_close = close.shift(1)
    tr1 = high - low
    tr2 = (high - prev_close).abs()
    tr3 = (low - prev_close).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    # Calculate ATR (EMA, similar to Wilder’s smoothing)
    atr = tr.ewm(span=period, adjust=False).mean()
    return atr


