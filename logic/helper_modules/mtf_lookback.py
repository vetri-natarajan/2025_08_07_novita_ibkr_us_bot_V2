# file: logic/helpers/lookback.py
"""
Lookback resolution for MTF checks.

Translates the configured higher/medium timeframe pairing and HHLL bars
into the number of bars to slice for evaluation.
"""


def resolve_mtf_lookback(symbol, main_settings, logger):
    """
    Compute MTF lookback bars from TF pairing.

    Parameters:
        symbol (str): Instrument key in main_settings.
        main_settings (dict): Settings dict with 'HHLL' and 'Parsed TF'.
        logger: Logger for context messages.

    Returns:
        int: Number of bars to look back for MTF checks.

    Raises:
        ValueError: When the configured higher and medium TFs are not supported.
    """
    HH_LL_bars = int(main_settings[symbol]['HHLL'])
    parsed_HTF = main_settings[symbol]["Parsed TF"][0]
    parsed_MTF = main_settings[symbol]["Parsed TF"][1]

    # Map supported TF pairings to a multiplier on HH_LL_bars.
    if parsed_HTF == '1 week' and parsed_MTF == '1 day':
        mtf_look_back = HH_LL_bars * 5  # Approximate trading days in a week
    elif parsed_HTF == '30 mins' and parsed_MTF == '5 mins':
        mtf_look_back = HH_LL_bars * 6  # 30m / 5m = 6 bars
    elif parsed_HTF == '3 mins' and parsed_MTF == '2 mins':
        mtf_look_back = HH_LL_bars * 5  # Chosen domain rule
    else:
        # Preserve original behavior: log and raise on mismatch.
        logger.info("❌ MTF Higher and medium time frame mismatch detected 🚩")
        raise ValueError("❌ MTF Higher and medium time frame mismatch detected 🚩")

    return mtf_look_back
