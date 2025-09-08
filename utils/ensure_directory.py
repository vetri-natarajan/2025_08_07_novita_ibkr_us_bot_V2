# -*- coding: utf-8 -*-
"""
utils/ensure_directory
"""
from pathlib import Path

def ensure_directory(directory_path : str , logger) -> Path:
    """
    Ensure that a directory exists.
    Creates it (including parents) if missing.
    
    Args:
        path (str): Path of the directory to check/create.
    
    Returns:
        Path: The Path object of the directory.
    """
    try:
        directory = Path(directory_path)
        directory.mkdir(parents=True, exist_ok=True)
        logger.info(f"📂 Directory ready: {directory}")
        return directory
    
    except Exception as e:
        logger.error(f"❌ Failed to create directory '{directory_path}': {e}")
        return None
    
    
    
    
    


