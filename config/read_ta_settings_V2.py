# -*- coding: utf-8 -*-
"""
Created on Sun Aug 10 11:38:45 2025

@author: Vetriselvan
"""
import os
import pandas as pd
import csv



def read_ta_settings(symbol, config_dir, logger):
    """
    Reads the TA settings csv and returns a dictionary keyed by indicators.
    
    file_path: Path to the .csv file
    logger: Logger instance for info messages
    """
    logger.info("📄🔍 Reading TA settings...")
    
    filename = f"{symbol}_Scalping_TA_Settings.csv"
    file_path = os.path.join(config_dir, filename)
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"TA settings file not found: {file_path}")

    with open(file_path, newline='') as csvfile:
        reader = csv.reader(csvfile)
        
        # First row: read symbol and headers
        first_row = next(reader)
        symbol = first_row[0]
        headers = first_row[1:]  # rest of first row are headers
        
        data = {}

        for row in reader:
            indicator = row[0]        # Use first column (indicator name) as key
            value = row[1]            # keep raw string as is
            other_values = row[2:]

            # Create dict for other columns keyed by headers (excluding 'Value')
            other_columns_dict = dict(zip(headers[1:], other_values))

            data[indicator] = {
                "value": value,
                "other_columns": other_columns_dict
            }

    logger.info(f"✅ Successfully read TA settings for symbol {symbol} 🎯")
    return data


