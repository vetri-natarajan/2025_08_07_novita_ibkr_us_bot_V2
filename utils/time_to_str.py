# -*- coding: utf-8 -*-
"""
utils/time_to_str

"""
from datetime import datetime
def time_to_str(time:datetime, only_date = False):
    """
    Converts a datetime object to a string formatted as 'YYYY-MM-DD_HH-MM-SS',
    suitable for use in filenames.
    
    Args:
      dt (datetime): The datetime object to format.
    
    Returns:
      str: The formatted datetime string.
     """
    if only_date:        
        return time.date().strftime('%Y_%m_%d')
    else:        
        return time.date().strftime('%Y-%m-%d_%H-%M-%S')   