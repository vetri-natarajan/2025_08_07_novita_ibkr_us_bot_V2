"""
execution/position_sizer.py

Purpose:
- Decide how many shares/contracts to buy for a signal, based on capital and VIX adjustments.
"""
import math
import logging



def compute_qty(account_value: float,
                percent_of_account_value: float, 
                units: int,
                price: float,
                vix: float,
                logger: logging,
                vix_threshold: float = 20.0,
                vix_reduction_factor: float = 0.5,
                skip_on_high_vix: bool = False,
                ) -> int:
    if price <= 0:
        return 0
    unit_value = account_value*percent_of_account_value*0.01/ max(1, units)
    #print("====================================")
    #print(vix, vix_reduction_factor)
    if vix >= vix_threshold:
        if skip_on_high_vix:
            logger.info("⚠️ High VIX %.2f >= %.2f and skip_on_high_vix enabled -> 0 qty", vix, vix_threshold)
            return 0
        else:
            unit_value = unit_value * vix_reduction_factor
    qty = math.floor(unit_value / price)
    if qty < 0:
        qty = 0
    return int(qty)
