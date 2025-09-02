def check_total_gain_limits(gains, limits):
    """
    Check that total gain sum is within specified limits.
    
    :param gains: Pandas Series of gains (percentages).
    :param limits: Tuple or list with (lower_limit, upper_limit).
    :return: True if total gain in range, else False.
    """
    total_gain = gains.sum()
    return limits[0] < total_gain < limits[1]