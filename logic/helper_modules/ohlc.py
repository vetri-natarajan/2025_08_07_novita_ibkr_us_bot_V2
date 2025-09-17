# file: logic/helpers/ohlc.py
"""
OHLC extraction utilities.

Provides typed series for vectorized numerical operations over OHLC data.
"""

def extract_ohlc_as_float(df):
    """
    Extract OHLC columns as float Series.

    Parameters:
        df (pd.DataFrame): Must contain 'open', 'high', 'low', 'close'.

    Returns:
        tuple[pd.Series, pd.Series, pd.Series, pd.Series]:
            (opens, highs, lows, closes) as float-typed Series.

    Raises:
        KeyError: If required columns are missing in df.
    """
    # astype(float) ensures consistent numeric dtype for downstream math.
    opens = df['open'].astype(float)
    highs = df['high'].astype(float)
    lows = df['low'].astype(float)
    closes = df['close'].astype(float)
    return opens, highs, lows, closes
