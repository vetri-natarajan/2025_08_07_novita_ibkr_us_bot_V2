def check_htf_structure(closes, opens, logger):
    """
    Check all closes greater than open.
    
    :param closes: Pandas Series of closes 
    :param opens: Pandas Series of opens
    :return: True if all clsoes greater than opens, else False.
    """
    if not all(closes > opens):
        logger.info("❌ HTF: Not all closes > opens")
        return False
    return True