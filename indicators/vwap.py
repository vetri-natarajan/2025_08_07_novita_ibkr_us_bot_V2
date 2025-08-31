"""
indicators/vwap
purpose: 
    - Provide VWAP calculation as a resuable fucntions
"""

import pandas as pd


def calculate_vwap(df: pd.DataFrame()) -> pd.Series():
    #df['high'] = df['high'][-(look_back + 1) : -1]
    #df['low'] = df['low'][-(look_back + 1) : -1]
    #df['close'] = df['close'][-(look_back + 1) : -1]
    
    df['high'] = df['high']
    df['low'] = df['low']
    df['close'] = df['close']
    tp = (df['high'] + df['low'] + df['close']) / 3.0
    pv = tp * df['volume']
    csum = pv.cumsum()
    vsum = df['volume'].cumsum()
    return csum / vsum
    