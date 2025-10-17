def calculate_gains(opens, closes):
    """
    Calculates gains
    
    :param closes: Pandas Series of closes 
    :param opens: Pandas Series of opens
    :return: gains.
    """
    return (closes - opens)*100/(opens + 1e-8)