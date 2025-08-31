# -*- coding: utf-8 -*-
"""
Created on Sun Aug 10 11:38:45 2025

@author: Vetriselvan
"""
import os
import pandas as pd

def read_ta_settings(file_path, logger):
    """
    Reads the TA settings csv and returns a list of dicts.
    
    file_path: Path to the .csv
    """
    logger.info("📄🔍 Reading TA settings...")
    
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"TA settings file not found: {file_path}")
    
    df = pd.read_csv(file_path)     
    
    # Drop empty columns, strip column names
    df.columns = [col.strip() for col in df.columns]
    df = df.dropna(how="all")
    
    # Convert to list of dictionaries for easier processing
    settings_list = df.to_dict(orient="records")   
    logger.info("✅ Successfully read TA settings 🎯")
    return settings_list