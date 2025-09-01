import logging
import os
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler

def initialize_logger(log_directory, logger_name, log_level=logging.INFO, mode=None, backup_count=0):
    """
    Set up a logger with console output and daily rotating log files named with the date.
    """
    print("\n🛠️ Setting up the logger 📝")
    os.makedirs(log_directory, exist_ok=True)
    logger = logging.getLogger(logger_name)
    logger.setLevel(log_level)
    if logger.hasHandlers():
        logger.handlers.clear()
    
    # Include current date in log filename
    log_file_path = os.path.join(log_directory, f"{logger_name}_{datetime.now().strftime('%Y-%m-%d')}.log")
    
    file_handler = TimedRotatingFileHandler(
        log_file_path, when="midnight", interval=1, backupCount=backup_count, encoding='utf-8'
    )
    file_handler.setLevel(log_level)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    # Add console handler only if mode is not 'backtest'
    print("mode in logger===>", mode)
    if mode != 'BACKTEST' or True:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(log_level)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
    print(f"\n📝✅ Logger setup successfully at {log_file_path} with level {logging.getLevelName(log_level)}")
    if backup_count != 0:
        print(f"🗃️ Logs will rotate daily; {backup_count} days of backups will be kept.")
    else:
        print(f"🗃️ Logs will rotate daily; Backups will be kept indefinitely.")
    if mode == 'BACKTEST':
        print("ℹ️ Console logging disabled due to 'BACKTEST' mode.")
    return logger
