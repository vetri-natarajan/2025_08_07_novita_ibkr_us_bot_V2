"""
logic/risk_management/atr_based_sl_tp.py

Purpose:
- Compute SL and TP levels using ATR multipliers.
"""
def compute_atr_sl_tp(price: float, atr_value: float, k_sl: float = 1.0, k_tp: float = 3.0):
    sl = round(price - (atr_value * k_sl))
    tp = round(price + (atr_value * k_tp))
    #sl = price - (atr_value * k_sl)
    #tp = price + (atr_value * k_tp)
    return sl, tp
