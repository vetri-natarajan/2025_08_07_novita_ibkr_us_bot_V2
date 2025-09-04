# -*- coding: utf-8 -*-
"""
Created on Thu Sep  4 08:04:28 2025

@author: Vetriselvan
"""

import re
def sanitize_filename(s):
    return re.sub(r'[^a-zA-Z0-9_\-\.]', '_', s)