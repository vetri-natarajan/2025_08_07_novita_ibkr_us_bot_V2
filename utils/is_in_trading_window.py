# -*- coding: utf-8 -*-
"""
utils/is_time_in_trading_windows
"""

from datetime import datetime, time

from datetime import datetime, time

def is_time_in_trading_windows(check_time: time, time_windows: list) -> bool:
    # time_windows is a list of strings like ['09:00-11:30', '11:30-19:00']
    for window in time_windows:
        start_str, end_str = window.split("-")
        start_time = datetime.strptime(start_str.strip(), "%H:%M").time()
        end_time = datetime.strptime(end_str.strip(), "%H:%M").time()
        if start_time <= check_time < end_time:
            return True
    return False
