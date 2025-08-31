"""
logic/risk_management/fixed_sl_tp.py

Purpose:
- Compute fixed percentage-based SL/TP. Simple fallback when ATR unavailable.
"""
def compute_fixed_sl_tp(price: float, sl_pct: float = 0.01, tp_pct: float = 0.03):
    sl = round(price * (1 - sl_pct))
    tp = round(price * (1 + tp_pct))
    return sl, tp
