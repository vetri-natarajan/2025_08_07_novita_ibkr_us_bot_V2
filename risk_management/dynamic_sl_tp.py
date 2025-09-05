"""
logic/risk_management/dynamic_sl_tp.py

"""
def compute_dynamic_sl_swing(entry_price, current_price, record):
    """
    Dynamic SL and TP calculation for Swing trade (E3) - Long only.

    Parameters:
    - entry_price: float, original entry price
    - current_price: float, latest market price
    - record: dict, trade record holding current state including profit_taken, qty
    
    Returns a dict with:
    - stop_loss: float, updated SL price
    - take_70_profit: bool, signal to take 70% profit partial exit
    - exit_remaining: bool, signal to exit remaining position
    - sl_triggered: bool, if price hits SL level (stop out)
    - updated_qty: int, remaining qty after partial profit
    """
    stop_loss = entry_price * 0.97
    tp1 = entry_price * 1.06   # +6%
    tp2 = entry_price * 1.09   # +9%
    tp3 = entry_price * 1.12   # +12%

    sl_triggered = current_price < stop_loss

    if current_price >= tp1:
        stop_loss = entry_price

    take_70_profit = current_price >= tp2 and not record.get("profit_taken", False)
    exit_remaining = current_price >= tp3

    updated_qty = record.get("qty", 0)
    if take_70_profit:
        updated_qty = int(updated_qty * 0.3)  # remaining 30%
    if exit_remaining:
        updated_qty = 0

    return {
        "stop_loss": stop_loss,
        "sl_triggered": sl_triggered,
        "take_70_profit": take_70_profit,
        "exit_remaining": exit_remaining,
        "updated_qty": updated_qty
    }