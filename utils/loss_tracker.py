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

class loss_tracker_class:
    def __init__(self, loss_halt_count: int = 3, loss_halt_duration: int = 24, trade_state_file: str = "logs/trade_state_file.json"):
        self.loss_halt_count = loss_halt_count
        self.loss_halt_duration = loss_halt_duration     
        self.trade_state_file = trade_state_file
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
                self.halt_until = now + dt.timedelta(hours = self.halt_hours)
            self._save()
            
    def _save(self):
        data = {
            "consecutive_losses": self.consecutive_losses,
            "halted_until": self.halted_until.isoformat() if self.halted_until else None
            }
        
        with open(self.trade_state_file, "w") as file: 
            json.dump(data, file)
            
    def is_halted(self) -> bool:
        if self.halted_until:
            now = dt.datetime.now(dt.timezone.utc)
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