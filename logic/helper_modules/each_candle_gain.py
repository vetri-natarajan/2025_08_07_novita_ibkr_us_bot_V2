def check_each_candle_gains(gains, limits, logger):
    """
    Check that each candle's gain is within specified limits.
    
    :param gains: Pandas Series of gains (percentages).
    :param limits: Tuple or list with (lower_limit, upper_limit).
    :return: True if all gains within limits, else False.
    """
    gain_within_limits = all((gains > limits[0]) & (gains < limits[1]))
    if not gain_within_limits:
        logger.info(f"❌ HTF: Gains not in range {limits} \n gains: \n{gains}")
        return False
    return True