# -*- coding: utf-8 -*-
"""
Created on Tue Sep  2 10:27:10 2025

@author: Vetriselvan
"""

from ib_async import IB, Stock
import asyncio
import nest_asyncio
nest_asyncio.apply()

ib = IB()
ib.connect('127.0.0.1', 7497, clientId=110)

contract = Stock('SBIN', 'NSE', 'INR')

bars = ib.reqRealTimeBars(contract, 5, 'TRADES', useRTH=True)

def on_bar_update(bars, hasNewBar):
    if hasNewBar:
        print(bars[-1])  # Print the latest 5-second bar

bars.updateEvent += on_bar_update

ib.run()