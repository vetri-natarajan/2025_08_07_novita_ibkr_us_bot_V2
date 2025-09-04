# -*- coding: utf-8 -*-
"""
Created on Thu Sep  4 08:05:02 2025

@author: Vetriselvan
"""

def ensure_utc(dt_index_or_series):
    """
    Ensure a pandas DateTimeIndex or Series of datetimes is tz-aware in UTC.
    Works for both tz-naive and tz-aware inputs.
    """
    if dt_index_or_series.tz is None:
        return dt_index_or_series.tz_localize("UTC")
    else:
        return dt_index_or_series.tz_convert("UTC")