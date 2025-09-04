# -*- coding: utf-8 -*-
"""
Created on Wed Sep  3 07:20:06 2025

@author: Vetriselvan
"""

import asyncio

import eventkit as ev
import ib_async

# optional for notebooks
ib_async.util.startLoop()

# Process bars function - simply print the bar
async def process_bars(bar_iterator):
    async for bar in bar_iterator:
        # put your logic here
        print(bar)

# Initialize IB, Contract & Ticker
ib = ib_async.IB()
ib.connect(port=7497, clientId=100)
print(ib.isConnected())
# we will use SPY live data
contract = ib_async.Stock(symbol="SBIN", exchange="NSE", currency="INR")
ib.qualifyContracts(contract)

# request a ticker
ticker = ib.reqMktData(contract, "", False, False)

# just in case wait for the ticker to be ready
ib.sleep(2)

# Start bar streaming every 60 seconds
bar_stream = ticker.updateEvent.trades().timebars(ev.Timer(60))

# Iter ticker
# await process_bars(bar_stream.aiter())
bars_task = asyncio.create_task(process_bars(bar_stream.aiter()))