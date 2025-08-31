import pandas as pd
import numpy as np

def calculate_adx(df, period=14):
    high = df['high']
    low = df['low']
    close = df['close']

    # Calculate True Range (TR)
    df['tr'] = np.maximum.reduce([
        high - low,
        abs(high - close.shift()),
        abs(low - close.shift())
    ])

    # Calculate Directional Movement (+DM, -DM)
    df['up_move'] = high - high.shift()
    df['down_move'] = low.shift() - low

    df['+dm'] = np.where((df['up_move'] > df['down_move']) & (df['up_move'] > 0), df['up_move'], 0.0)
    df['-dm'] = np.where((df['down_move'] > df['up_move']) & (df['down_move'] > 0), df['down_move'], 0.0)

    # Smooth True Range and DM using Wilder’s smoothing (EMA with alpha = 1/period)
    tr_smooth = df['tr'].rolling(window=period).sum()
    plus_dm_smooth = df['+dm'].rolling(window=period).sum()
    minus_dm_smooth = df['-dm'].rolling(window=period).sum()

    # Calculate +DI and -DI
    plus_di = 100 * (plus_dm_smooth / tr_smooth)
    minus_di = 100 * (minus_dm_smooth / tr_smooth)

    # Calculate DX
    dx = 100 * (abs(plus_di - minus_di) / (plus_di + minus_di))

    # Calculate ADX - average of DX over 'period'
    adx = dx.rolling(window=period).mean()

    df['ADX'] = adx
    df['+DI'] = plus_di
    df['-DI'] = minus_di

    return df['ADX']

