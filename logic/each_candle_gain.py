def check_each_candle_gains(gains, limits):
    """
    Check that each candle's gain is within specified limits.
    
    :param gains: Pandas Series of gains (percentages).
    :param limits: Tuple or list with (lower_limit, upper_limit).
    :return: True if all gains within limits, else False.
    """
    return all((gains > limits[0]) & (gains < limits[1]))