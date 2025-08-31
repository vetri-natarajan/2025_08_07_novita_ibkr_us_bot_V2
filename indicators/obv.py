import pandas as pd
import numpy as np

def calculate_obv(df):
    """
    Calculates On-Balance Volume (OBV) for a DataFrame of closing prices and volume.

    Args:
        df (pd.DataFrame): DataFrame with columns 'close' and 'volume'.

    Returns:
        pd.Series: OBV values as a Pandas Series.
    """
    close = df['close']
    volume = df['volume']

    # Compute daily price movement direction
    direction = np.sign(close.diff())  # +1 for up, -1 for down, 0 for unchanged

    # Replace NaN in direction with 0 (first value)
    direction.iloc[0] = 0

    # Calculate OBV: Sum of volumes, adding or subtracting based on direction
    obv = (volume * direction).cumsum()

    return obv

