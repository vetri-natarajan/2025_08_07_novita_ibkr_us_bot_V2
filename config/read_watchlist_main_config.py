# -*- coding: utf-8 -*-
"""
Created on Sun Aug 10 11:38:45 2025

@author: Vetriselvan
"""
import os
import pandas as pd
import re

def return_gains(string):
    separators = ['?', r'/', ',']    
    sep = None
    
    for separator in separators:
        if separator in string:
            sep = separator
            break
    string_split = string.split(sep) 
    gain1 = float(string_split[0].strip())
    gain2 = float(string_split[1].strip())        
        
    return (gain1, gain2)
    

def parse_time_frames(hi_to_lo_tf_combined):
    raw_timeframes = [s.strip() for s in hi_to_lo_tf_combined.split(',')]
    pattern = re.compile(r"(\d+)([mwdhMWDH])")
    parsed = []
    for tf in raw_timeframes:
        if tf.lower() == "skip":
            parsed.append(("skip", "skip"))
            continue
        match = pattern.fullmatch(tf)
        if match: 
            value = int(match.group(1))
            unit = match.group(2)
            parsed.append((value, unit))
        else: 
            parsed.append((None, None))
    
    tf_string_list_parsed = []
    for tf in parsed:       
        number, unit = tf
    
        if number == "skip":
            tf_string_list_parsed.append("skip")
            continue
    
        # Map units to full words
        units_map = {
            'm': 'min',
            'h': 'hour',
            'd': 'day',
            'w': 'week',
        }
    
        # Normalize unit to lowercase
        unit_word = units_map.get(unit.lower(), unit)
        tf_string = ""
        # Handle pluralization
        if number == 1:
            tf_string =  f"{number} {unit_word}"
        elif number is None:
            tf_string = "invalid"
        else:
            tf_string = f"{number} {unit_word}s"
        tf_string_list_parsed.append(tf_string)
    return raw_timeframes, tf_string_list_parsed

    
def parse_total_gains(total_gains_raw):
    total_gains_stripped = total_gains_raw.strip().replace('%', "").replace('~', "").replace("'", "").replace('""', "")
    
    return return_gains(total_gains_stripped)
    
def parse_each_gains(each_gains_raw):
    each_gains_stripped = each_gains_raw.strip().replace('%', "").replace('~', "").replace("'", "").replace('""', "")

    return return_gains(each_gains_stripped)

def parse_each_vol(each_vol_raw):
    each_vol_stripped = each_vol_raw.strip().replace('%', "").replace('~', "").replace("'", "").replace('""', "")
    return return_gains(each_vol_stripped)





def read_watchlist_main_config(file_path, logger):
    """
    Reads the watchlist main configuration csv 
    and returns a list of dicts.
    
    file_path: Path to the .csv file.
    """
    logger.info("\n📄🔍 Reading watchlist_main_config settings...")
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"watchlist_main_config file not found: {file_path}")
    
    df = pd.read_csv(file_path) 
    
    # Drop empty columns, strip column names
    df.columns = [col.strip() for col in df.columns]
    #print(df.columns)
    df = df.dropna(how="all")

    logger.info('df in main settings ===========>\n')
    logger.info(df)    
    
    # Convert to list of dictionaries for easier processing
    settings_list = df.to_dict(orient="records")
  
    logger.info('settings list ===========>\n')
    logger.info(settings_list)  
    
    
    symbol_list = [item['Symbol'].upper() + '_' + item['Mode'].upper() for item in settings_list]    
    settings_by_symbol_dict = {item['Symbol'].upper() + '_' + item['Mode'].upper(): item for item in settings_list}
    
    logger.info('settings_by_symbol_dict===========>\n')
    logger.info(settings_by_symbol_dict)      
    
    logger.info("\n\n\n parsing the hi_to-lo tf")
    for k, v  in settings_by_symbol_dict.items():
        
        #logger.info("v before")
        #logger.info(v)
        parsed_tf = parse_time_frames(v.get("Hi to Lo TF"))
        v["Parsed Raw TF"] , v["Parsed TF"] = parsed_tf
        
        parsed_total_gains = parse_total_gains(v.get("Total Gain"))
        v["Parsed Total Gain"] = parsed_total_gains      

        parsed_each_gains = parse_each_gains(v.get("Each Gain"))
        v["Parsed Each Gain"] = parsed_each_gains   

        parsed_each_vol = parse_each_vol(v.get("Volatility"))
        v["Parsed Each Volatility"] = parsed_each_vol     
       # parsed_gains = parse_total_gains(v.get)
        #logger.info("v after")
        #logger.info(v)
        
    
    

    logger.info("parsed \n", settings_by_symbol_dict)
    logger.info(f"👀📊 Symbols in watchlist are {symbol_list}")
    logger.info("✅ Successfully read watchlist_main_config settings...🎯")
    return symbol_list, settings_by_symbol_dict


#parse_time_frames('30m, 5m, 1m')
#parse_total_gains("'~0.28% ? 0.70%'")
#x = parse_each_gains("~0.09%/ 0.23%")
#x[0]
#x[1]

