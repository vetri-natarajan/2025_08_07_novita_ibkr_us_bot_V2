import os
import csv

def read_ta_settings(symbol, inputs_directory, watchlist_main_settings, logger):
    """
    Reads the TA settings csv and returns a dictionary keyed by indicators,
    along with the max value and max lookback found in data.

    symbol: Symbol string, e.g. 'SBIN'
    config_dir: Directory containing CSV files
    logger: Logger instance for info messages

    Returns:
        data: dict of indicator settings
        max_value: float or None - max numeric value found in 'value' keys
        max_lookback: int or None - max lookback value in 'Lookback' keys
    """
    logger.info("📄🔍 Reading TA settings...")
    trading_mode = watchlist_main_settings[symbol]['Mode']
    if trading_mode.upper() == 'SCALPING':
        filename = f"{symbol}_Scalping_TA_Settings.csv"
    elif trading_mode.upper() == 'SWING':
        filename = f"{symbol}_Swing_TA_Settings.csv"
    else: 
        raise ValueError(f"⚠️ Invalid trading mode: {trading_mode}. Please choose a valid option.")

    file_path = os.path.join(inputs_directory, filename)
    
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"TA settings file not found: {file_path}")

    with open(file_path, newline='') as csvfile:
        reader = csv.reader(csvfile)

        first_row = next(reader)
        symbol = first_row[0]
        headers = first_row[1:]  # rest of first row are headers

        data = {}
        max_value = None
        max_lookback = None

        for row in reader:
            indicator = row[0]
            value = row[1]  # keep raw string as is
            other_values = row[2:]
            other_columns_dict = dict(zip(headers[1:], other_values))
            
            # Save parsed data
            data[indicator] = {
                "value": value,
                "other_columns": other_columns_dict
            }

            # Extract max_value from 'value' - handles comma-separated and % signs
            val_str = value
            if val_str:
                parts = [v.strip().replace('%','') for v in val_str.split(',')]
                nums = []
                for p in parts:
                    try:
                        nums.append(float(p))
                    except ValueError:
                        pass
                if nums:
                    val_num = max(nums)
                    if max_value is None or val_num > max_value:
                        max_value = val_num
            
            # Extract max_lookback
            lookback_str = other_columns_dict.get('Lookback', '')
            try:
                lookback_num = int(lookback_str)
                if max_lookback is None or lookback_num > max_lookback:
                    max_lookback = lookback_num
            except (ValueError, TypeError):
                pass
            
        #print(data)    
        for k, v in data.items(): 
            k = k.upper()
            #print("v['value']===>", v['value'])
            if k == "MACD":
                v["parsed_value"] = [int(item.strip()) for item in v['value'].split(",")]
                
            if k == "VWAP":
                v["parsed_value"] = float(v['value'].replace("%", ""))
                
            if k == "EMA":
                v["parsed_value"] = [int(item.strip()) for item in v['value'].split(",")]
                
            if k == "ADX":
                v["parsed_value"] = int(v['value'].strip())


            if k == "ATR":
                v["parsed_value"] = [int(item.strip()) for item in v['value'].split(",")]                   
                    
            if k == "OBV":
                v["parsed_value"] = float(v['value'].strip())

            

    logger.info(f"✅ Successfully read TA settings for symbol {symbol} 🎯")
    return data, max(max_value, max_lookback)
