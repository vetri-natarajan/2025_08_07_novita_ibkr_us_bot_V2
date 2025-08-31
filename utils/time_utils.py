# -*- coding: utf-8 -*-
"""
Created on Thu Aug 14 06:58:49 2025

@author: Vetriselvan
Purpose: parse trading windows and decide if current time is wintin allowd intervals
"""
import pytz
import datetime as dt

EASTERN = pytz.timezone("US/Eastern")

def parse_window(s: str):
    """
    Take a time range string "HH:MM-HH:MM" and convert it into numeric hour/minute tuples.

    Parameters
    ----------
    s : str
        start and end times

    Returns
    -------
    TYPE
        Tuples.

    """
    start_str, end_str = s.split("-")
    sh, sm = map(int, start_str.split(":"))
    eh, em = map(int, end_str.split(":"))
    return (sh, sm), (eh, em)
    

def in_trading_window(now_utc: dt.datetime, trading_window_config) -> bool:
    #ensuring time has timeinfo
    if not now_utc.tzinfo: 
        now_utc = now_utc.replace(tzinfo = dt.timezone.utc)
   
    now_local = now_utc.astimezone(EASTERN)
    trading_windows = trading_window_config   

    if isinstance(trading_window_config, str):
        trading_windows = [w.strip() for w in trading_window_config.split(",").strip() if w.strip()]

    for w in trading_windows:
        (sh, sm), (eh, em) = parse_window(w)
        start = now_local.replace(hour = sh, minute=sm, second=0, microsecond=0)
        end = now_local.replace(hour = eh, minute=em, second=0, microsecond=0)
        if start <= now_local <= end:
            return True
        
    return False
        
        
        