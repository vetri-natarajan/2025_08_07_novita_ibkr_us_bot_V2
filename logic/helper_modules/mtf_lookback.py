# file: logic/helpers/lookback.py
"""
Lookback resolution for MTF checks.

Translates the configured higher/medium timeframe pairing and HHLL bars
into the number of bars to slice for evaluation.
"""


def resolve_mtf_lookback(symbol_combined, main_settings, logger):
    """
    Compute MTF lookback bars from TF pairing.

    Parameters:
        symbol (str): Instrument key in main_settings.
        main_settings (dict): Settings dict with 'HHLL' and 'Parsed TF'.
        logger: Logger for context messages.

    Returns:
        int: Number of bars to look back for MTF checks.

    Raises:
        ValueError: When the configured higher and medium TFs are not supported or MTF lookback is less than 1
    """
    
    
    try:
        mtf_look_back = int(main_settings[symbol_combined]['MTF lookback'])
        
        if mtf_look_back < 1:
            logger.info("❌ MTF lookback must be greater than 0 🚩")
            raise ValueError("❌ MTF lookback must be greater than 0  🚩")
        else: 
            mtf_look_back = mtf_look_back + 1
            

    except Exception as e:

        logger.info(f"⚠️ Exception occurred in mtf_lookback: {e}")
        logger.info("❌ Check MTF Lookback input 🚩")
        raise ValueError("❌ Check MTF Lookback input  🚩")

    return mtf_look_back
