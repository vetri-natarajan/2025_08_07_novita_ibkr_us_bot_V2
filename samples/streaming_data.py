import asyncio
import logging
from ib_async import IB, Stock, util
import eventkit as ev

# Set up detailed logging for debugging
logging.basicConfig(
    level=logging.INFO,  # Use DEBUG for even more detail
    format='%(asctime)s [%(levelname)s]: %(message)s'
)
logger = logging.getLogger(__name__)

async def main():
    logger.info("Starting IBKR API session...")
    ib = IB()
    try:
        await ib.connectAsync('127.0.0.1', 7497, clientId=123)
        logger.info("Connected to IBKR server.")

        # Define your contract
        contract = Stock("SBIN", "NSE", "INR")
        await ib.qualifyContractsAsync(contract)
        logger.info(f"Qualified contract: {contract}")

        # Request live market data
        ticker = ib.reqMktData(contract, "", False, False)
        logger.info("Market data request sent. Waiting for updates...")
        await asyncio.sleep(2)  # Wait for ticker to initialize

        # Create 60-second bars from trade events
        bar_stream = ticker.updateEvent.trades().timebars(ev.Timer(60))
        logger.info("Started bar streaming.")

        async for bar in bar_stream.aiter():
            logger.info(f"Received bar: {bar}")

    except Exception as e:
        logger.error(f"Error during IBKR session: {e}")
    finally:
        if ib.isConnected():
            ib.disconnect()
            logger.info("Disconnected from IBKR server.")

if __name__ == "__main__":
    # Start event loop and run
    util.startLoop()  # Only needed if running interactively (like in Jupyter)
    asyncio.run(main())
