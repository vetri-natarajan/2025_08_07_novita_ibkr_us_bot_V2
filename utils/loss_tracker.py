# -*- coding: utf-8 -*-
"""
Created on Wed Aug 13 08:52:00 2025

@author: Vetriselvan

Track consecutive losses and temporarily halt trading for 24 hrs
"""
import os
import json
import datetime as dt
from typing import Optional
import logging

class loss_tracker_class:
    def __init__(self, loss_halt_count: int = 3, loss_halt_duration: int = 24, trade_state_file: str = "logs/trade_state_file.json", logger:logging = None):
        self.loss_halt_count = loss_halt_count
        self.loss_halt_duration = loss_halt_duration     
        self.trade_state_file = trade_state_file
        self.logger = logger
        self._load()
        
    def _load(self):
        '''
        loads the contents of the trade_state json file

        Returns
        -------
        None.

        '''
        if os.path.exists(self.trade_state_file):
            with open(self.trade_state_file, "r") as file: 
                data = json.load(file)
                print(data)
                self.consecutive_losses = data.get("consecutive_losses", 0)
                halted_until_temp = data.get("halted_until")
                self.halted_until = dt.datetime.fromisoformat(halted_until_temp) if halted_until_temp else None
        else: 
            self.consecutive_losses = 0
            self.halted_until = None
            
    def add_trade_result(self, win: bool, timestamp: Optional[dt.datetime] = None):
        
        if win:
            self.consecutive_losses = 0
        else: 
            self.consecutive_losses += 1
            if self.consecutive_losses >= self.loss_halt_count:
                now = timestamp or dt.datetime.now(dt.timezone.utc)
                self.halted_until = now + dt.timedelta(hours = self.loss_halt_duration)
            self._save()
            self.logger.info(f"🧾 Trade result saved to {self.trade_state_file} — consecutive losses: {self.consecutive_losses} self.halted_until {self.halted_until}")            
            
    def _save(self):
        data = {
            "consecutive_losses": self.consecutive_losses,
            "halted_until": self.halted_until.isoformat() if self.halted_until else None
            }
        
        with open(self.trade_state_file, "w") as file: 
            json.dump(data, file)
            
    def is_halted(self, entry_time: Optional = None) -> bool:
        self.logger.info(f"🕒 entry time {entry_time}")
        if self.halted_until:
            if entry_time is None: 
                now = dt.datetime.now(dt.timezone.utc)
            else: 
                now = entry_time
            self.logger.info(f"ℹ️ Checking halt status — halted_until: {self.halted_until}, entrytime: {entry_time} now: {now} 🕒")
            
            if now < self.halted_until:
                return True
            else: 
                self.halted_until = None
                self.consecutive_losses = 0
                self._save()
                return False
        return False
    
    def halt_until(self) -> Optional[str]:
        return self.halted_until.isoformat() if self.halted_until else None