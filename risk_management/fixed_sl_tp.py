"""
logic/risk_management/fixed_sl_tp.py

Purpose:
- Compute fixed percentage-based SL/TP. Simple fallback when ATR unavailable.
"""
def compute_fixed_sl_tp(price: float, sl_pct: float = 2, tp_pct: float = 4):
    sl = round(price * (1 - (sl_pct*0.01)))
    tp = round(price * (1 +  (tp_pct*0.01)))
    return sl, tp
