# -*- coding: utf-8 -*-
"""
Created on Tue Aug 12 08:51:22 2025

@author: Vetriselvan
"""

import asyncio
import datetime as dt
from ib_async import IB, Stock

import nest_asyncio
nest_asyncio.apply()

# ====================== CONFIG ====================== #
# Example configuration — replace with your own
config_dict = {
    "account_type": "PAPER",   # or "LIVE"
    "ib_paper_host": "127.0.0.1",
    "ib_paper_port": 7497,
    "ib_live_host": "127.0.0.1",
    "ib_live_port": 4003,
    "client_id": 5
}

# Create the IB instance ONCE (shared throughout the app)
ib = IB()


# ====================== CONNECTOR CLASS ====================== #
class IBKRConnector:
    def __init__(self, account_type, ib_instance: IB):
        self.account_type = account_type
        self.ib = ib_instance
        self.is_connected = False

    async def connect(self):
        """
        Keep connecting to IB until successful, automatically reconnect if needed.
        """
        send_first_error = True  

        while True:
            try:
                if not self.ib.isConnected():
                    self.is_connected = False

                    if self.account_type.upper() == "PAPER":
                        await self.ib.connectAsync(
                            config_dict["ib_paper_host"],
                            config_dict["ib_paper_port"],
                            clientId=config_dict["client_id"],
                            timeout=4
                        )

                    elif self.account_type.upper() == "LIVE":
                        await self.ib.connectAsync(
                            config_dict["ib_live_host"],
                            config_dict["ib_live_port"],
                            clientId=config_dict["client_id"],
                            timeout=4
                        )

                    self.is_connected = True
                    print(f"\n{dt.datetime.now()} 🔗 ✅ Connected to IB.")
                    send_first_error = True  # reset

                await asyncio.sleep(1)  # Poll connection status every second

            except Exception as e:
                print(f"❌ Connection error: {e}. 🔄 Retrying...")
                if send_first_error:
                    self.is_connected = False
                    send_first_error = False
                await asyncio.sleep(5)  # Wait before retry

    async def disconnect(self):
        """Manual disconnect (clean shutdown)"""
        if self.ib.isConnected():
            self.ib.disconnect()
            self.is_connected = False
            print(f"{dt.datetime.now()} 🔌 Disconnected from IB.")


# ====================== MAIN PROGRAM ====================== #
async def request_market_data():
    """
    Example: Reusing the connected 'ib' instance for market data.
    """
    contract = Stock('AAPL', 'SMART', 'USD')
    market_data = ib.reqMktData(contract)

    # Wait a moment for data to arrive
    await asyncio.sleep(2)

    print(f"📊 Market Data: Bid={market_data.bid}, Ask={market_data.ask}")

async def main():
    connector = IBKRConnector(config_dict["account_type"], ib)

    # Start connection handler in the background
    asyncio.create_task(connector.connect())

    # Wait until IB is connected before requesting data
    while not connector.is_connected:
        await asyncio.sleep(0.2)

    # Now reuse the same global IB object for market data
    await request_market_data()

    
    # Keep running other trading logic here...
    while True:
        await asyncio.sleep(10)  # Just keep the app alive for now


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("🛑 Terminated by user.")
