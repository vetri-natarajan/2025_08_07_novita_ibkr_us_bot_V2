"""
data/vix_spx.py

Purpose: 
    - provide functions to fetch VIX and SPX data from IB- for pre-marekt checks
    Does the following: 
        1. get vix data
        2. get spx data
        3. get spx open
        4. checks whether vix falls outside the range
"""
import asyncio
from ib_async import Index, Stock

async def get_vix(ib, exchange, currency, vix_symbol, logger):
    logger.info("🔍 Initiating VIX data fetch: exchange=%s, currency=%s", exchange, currency)
    vix_contract = Index(symbol=vix_symbol, exchange=exchange, currency=currency)
    
    try:
        ticker = ib.reqMktData(vix_contract, snapshot=True, regulatorySnapshot=False)
        logger.info("🛰️ Requested market data for VIX contract.")
        await asyncio.sleep(1)
        logger.info("⏲️ Waited 0.5s for VIX market data response.")

        value = None
        if getattr(ticker, 'last', None):
            value = float(ticker.last)
            logger.info("✅ Retrieved VIX last price: %s", value)
        elif getattr(ticker, 'close', None):
            value = float(ticker.close)
            logger.info("ℹ️ Used VIX close price instead: %s", value)
        else:
            logger.warning("⚠️ Neither last nor close price available for VIX ticker.")

        ib.cancelMktData(vix_contract)
        logger.info("🛑 Cancelled VIX market data subscription.")

        if value is None:
            logger.error("🛑 SYSTEM HALTED: Could not fetch VIX ❌")
            raise RuntimeError("🛑 SYSTEM HALTED: Could not fetch VIX ❌")

        logger.info("📈 Returning VIX value: %s", value)
        return value
    except Exception as e:
        logger.exception("🚨 Exception occurred while fetching VIX: %s", e)
        raise

async def get_spx(ib, exchange, currency, spx_symbol, logger):
    logger.info("🔍 Initiating SPX data fetch: exchange=%s, currency=%s", exchange, currency)
    spx_contract = Index(symbol=spx_symbol, exchange=exchange, currency=currency)
    
    try:
        ticker = ib.reqMktData(spx_contract, snapshot=True, regulatorySnapshot=False)
        logger.info("🛰️ Requested market data for SPX contract.")
        await asyncio.sleep(0.5)
        logger.info("⏲️ Waited 0.5s for SPX market data response.")

        value = None
        if getattr(ticker, 'last', None):
            value = float(ticker.last)
            logger.info("✅ Retrieved SPX last price: %s", value)
        elif getattr(ticker, 'close', None):
            value = float(ticker.close)
            logger.info("ℹ️ Used SPX close price instead: %s", value)
        else:
            logger.warning("⚠️ Neither last nor close price available for SPX ticker.")

        ib.cancelMktData(spx_contract)
        logger.info("🛑 Cancelled SPX market data subscription.")

        if value is None:
            logger.error("🛑 SYSTEM HALTED: Could not fetch SPX ❌")
            raise RuntimeError("🛑 SYSTEM HALTED: Could not fetch SPX ❌")

        logger.info("📈 Returning SPX value: %s", value)
        return value
    except Exception as e:
        logger.exception("🚨 Exception occurred while fetching SPX: %s", e)
        raise

async def get_spx_close(ib, exchange, currency, logger):
    logger.info("🔍 Fetching SPX previous close: exchange=%s, currency=%s", exchange, currency)
    spy_contract = Index(symbol="SPX", exchange=exchange, currency=currency)
    
    try:
        bars = ib.reqHistoricalData(
            spy_contract,
            endDateTime='',
            durationStr='1 D',
            barSizeSetting='1 day',
            whatToShow='TRADES',
            useRTH=True,
        )
        logger.info("📊 Requested 1-day historical data for SPX.")

        if not bars:
            logger.warning("⚠️ No historical bars returned for SPX.")
            return None

        spy_close = float(bars["close"][-1])
        logger.info("✅ Fetched SPX close price: %s", spy_close)
        return spy_close
    except Exception as e:
        logger.exception("🚨 Exception during SPX close fetch: %s", e)
        raise

def rule_of_16_calculation(vix_value, spx_close, spx_current, logger):
    logger.info(
        "🧮 Calculating rule of 16: vix_value=%s, spx_close=%s, spx_current=%s",
        vix_value, spx_close, spx_current
    )
    expected_range = spx_close * vix_value / 16
    expected_trading_range_upper = spx_close + expected_range
    expected_trading_range_lower = spx_close - expected_range

    logger.info(
        "📐 Expected range: lower=%s, upper=%s",
        expected_trading_range_lower, expected_trading_range_upper
    )

    within_range = expected_trading_range_lower < spx_current < expected_trading_range_upper
    logger.info(
        "📊 SPX current price %s within expected range? %s",
        spx_current, "✅ YES" if within_range else "❌ NO"
    )
    return within_range
