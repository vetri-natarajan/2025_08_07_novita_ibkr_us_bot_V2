def check_max_drawdown(highs, lows, max_allowed, logger):
    """
    Calculate drawdown and check if it exceeds max allowed percent.
    
    :param highs: Pandas Series of highs.
    :param lows: Pandas Series of lows.
    :param max_allowed: Max drawdown percent allowed.
    :return: True if drawdown <= max_allowed, else False.
    """
    peak = highs.max()
    trough = lows.min()
    drawdown = (peak - trough)*100/(peak + 1e-8)
    drawdown_within_limits = drawdown <= max_allowed
    if not drawdown_within_limits:
        logger.info("❌ HTF: Drawdown condition failed")
        return False
    return True
