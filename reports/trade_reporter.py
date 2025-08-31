"""
reporter/trade_reporter.py
- Create a CSV file containing trade events for audit
"""
import os
import csv
from datetime import datetime, timezone

class trade_reporter_class():
    def __init__(self, trade_reporter_file : str, logger):
        self.logger = logger
        logger.info("⚙️ Initializing trade reporter class...")
        self.file_name = trade_reporter_file
        folder : str = os.path.dirname(self.file_name) # folder path string
        if folder: 
            os.makedirs(folder, exist_ok=True)
        if not os.path.exists(self.file_name):
            with open(self.file_name, "w", newline = "") as file:
                writer = csv.writer(file)
                writer.writerow(["timestamp", "trade_id", "symbol", "side", "qty", "entry_price", "exit_price", "pnl", "meta"])
        self.logger.info("✅ Trade reporter initialization done ...")
    
    def report_trade_open(self, trade_id: str, record: dict):
        ts = datetime.now(timezone.utc).isoformat()
        symbol = record["contract"].symbol
        side = record["side"]
        qty = record["qty"]
        fill_price = record["fill_price"]
        exit_price = ""
        pnl = ""
        meta = str(record["meta"])
        
        self.logger.info("📈 Trade opened: %s %s", trade_id, record)  
        with open(self.file_name, "w") as file: 
            writer = csv.writer(file)
            writer.writerow([ts, trade_id, symbol, side, qty, fill_price, exit_price, pnl, meta])
            


    def report_trade_close(self, trade_id: str, record: dict):
        ts = datetime.now(timezone.utc).isoformat()
        symbol = record["contract"].symbol
        side = record["side"]
        qty = record["qty"]
        fill_price = record["fill_price"]
        exit_price = None
        pnl = ""
        meta = str(record["meta"])
        
        self.logger.info("📉 Trade closed: %s %s", trade_id, record)  
        with open(self.file_name, "w") as file: 
            writer = csv.writer(file)
            writer.writerow([ts, trade_id, symbol, side, qty, fill_price, exit_price, pnl, meta])
  