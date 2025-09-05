"""
logic/risk_management/hedge_sl_tp.py


"""
from datetime import datetime, timedelta

def compute_hedge_exit_trade(entry_price, current_price, vwap, entry_time, current_time):
    """
    Dynamic SL and TP for hedge exit (E4) - Long only.

    Parameters:
    - entry_price: float
    - current_price: float
    - vwap: float
    - entry_time: datetime
    - current_time: datetime

    Returns dict with:
    - stop_loss: float
    - sl_triggered: bool
    - take_profit: bool
    - auto_close: bool (after 5 days)
    """
    max_holding_days = 5
    auto_close = (current_time - entry_time).days >= max_holding_days

    stop_loss = vwap * 0.99
    sl_triggered = current_price < stop_loss
    take_profit = current_price > entry_price * 1.1515

    return {
        "stop_loss": stop_loss,
        "sl_triggered": sl_triggered,
        "take_profit": take_profit,
        "auto_close": auto_close
    }