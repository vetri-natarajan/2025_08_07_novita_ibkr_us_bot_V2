from ib_async import IB, MarketOrder, StopOrder, LimitOrder
from typing import Dict, Any
import asyncio
import json
import os
import datetime
from data.market_data import MarketData

class order_manager_class:
    def __init__(self, ib : IB, 
                 trade_reporter, 
                 loss_tracker, 
                 order_manager_state_file, 
                 trade_timeout_secs,
                 auto_trade_save_secs, 
                 config_dict,
                 logger):
        
        self.logger = logger
        self.logger.info("⚙️ Initializing order manager class")
        
        self.ib = ib
        self.trade_reporter = trade_reporter
        self.loss_tracker = loss_tracker
        self.order_manager_state_file = order_manager_state_file
        self.trade_timeout_secs = trade_timeout_secs
        self.auto_trade_save_secs = auto_trade_save_secs
        self.active_trades: Dict[str, Dict[str, Any]] = {}
        self._state_task = asyncio.create_task(self._periodic_state_save())

        self.portfolio_snapshot_interval_secs = 30 * 60  # 30 minutes initially
        self._portfolio_snapshot_task = asyncio.create_task(self._periodic_portfolio_snapshot())
        
        self.exchange = config_dict['exchange']
        self.currency = config_dict['currency']
        self.logger.info("✅ Order manager initialized successfully")
        
    async def place_market_entry_with_bracket(self, contract, 
                                              qty: int, 
                                              side: str, 
                                              sl_price: float = None,
                                              tp_price: float = None, 
                                              meta : Dict = None):
        if qty < 0: 
            self.logger.warning("⚠️ Quantity <= 0, skipping order placement")
            return None            
        
        order = MarketOrder(side.upper(), qty)
        parent_order =  self.ib.placeOrder(contract, order)
        await parent_order.filledEvent
        
        order_id = getattr(parent_order.orderStatus, "permId", None)
        symbol_temp = getattr(contract, "symbol", "UNKNOWN")
        trade_id = f"{symbol_temp}-{order_id}"
        record = {
                "contract": contract,
                "order": order,
                "order_id": order_id,
                "qty": qty,
                "side": side.upper(),
                "sl_price": sl_price,
                "tp_price": tp_price,
                "meta": meta or {},
                "state": "PENDING",
                "fill_price": None
                }
        
        self.active_trades[trade_id] = record
        self.logger.info("✅ Placed parent market order %s for %s qty %s", trade_id, symbol_temp, qty)
        
        asyncio.create_task(self._monitor_fill_and_attach_children(trade_id))
        await self.save_state()
        return trade_id
       
    async def _monitor_fill_and_attach_children(self, trade_id:str):
        record = self.active_trades.get(trade_id)
        if not record:
            self.logger.warning("⚠️ No active trade record found for %s; nothing to monitor.", trade_id)
            return
        contract = record["contract"]
        order_id = record.get("order_id")
        qty = record.get("qty")
        sl_price = record.get("sl_price")
        tp_price = record.get("tp_price")
        
        for i in range(self.trade_timeout_secs*2):
            await asyncio.sleep(0.5)
            trades = self.ib.trades()
            parent_trade = None
            for t in trades: 
                if getattr(t.order, "permId", None) == order_id:
                    parent_trade = t
                    break
            if parent_trade:
                if parent_trade.isDone():
                    record["state"] = "FILLED"
                    fill_price = None
                    try: 
                        if parent_trade.fills:
                            fill_price = parent_trade.fills[-1].execution.price
                    except Exception as e:
                        self.logger.info(f"⚠️ Exception occurred in monitor fill and attach module {e}, passing.")                        
                    record["fill_price"] = fill_price
                    self.logger.info("✅ Parent order filled for %s at price %s", trade_id, fill_price)
            
            print("record===>")
            print(record)
            print('sl_price=====>')    
            print(record.get("sl_price"))
            if record.get("sl_price") is not None: 
                sl_side = "SELL" if record["side"] == "BUY" else "BUY"
                stop_order = StopOrder(sl_side, qty, sl_price)
                try: 
                    sl_trade = self.ib.placeOrder(contract, stop_order)
                    record["sl_order_id"] = getattr(sl_trade.orderStatus, "permId", None)
                    self.logger.info("✅  Placed Stop order for %s at %s", trade_id, sl_price)
                except Exception as e:
                    self.logger.exception("❌ Failed to place Stop order for %s", trade_id)
        
            if record.get("tp_price") is not None: 
                tp_side = "SELL" if record["side"] == "BUY" else "BUY"
                limit_order = LimitOrder(tp_side, qty, tp_price)
                try: 
                    tp_trade = self.ib.placeOrder(contract, limit_order)
                    record["tp_order_id"] = getattr(sl_trade.orderStatus, "permId", None)
                    self.logger.info("✅  Placed Limit order for %s at %s", trade_id, tp_price)
                except Exception as e:
                    self.logger.exception("❌ Failed to place Limit order for %s", trade_id)      
                try: 
                    self.trade_reporter.report_trade_open(trade_id, record)
                
                except Exception as e: 
                    self.logger.exception("🚨 Failed to report trade open for %s exception %s", trade_id, e)           
                    
                asyncio.create_task(self._monitor_exit(trade_id))
                await self.save_state()
                return
            
            record["state"] = "FAILED"
            self.logger.warning("⏳❌ Parent order for %s did not fill within timeout; marked FAILED", trade_id)
            await self.save_state()
            return     
    
    async def _monitor_exit(self, trade_id: str):
        record = self.active_trades.get(trade_id)
        if not record:
            self.logger.warning("⚠️ No active trade record found for %s; nothing to monitor.", trade_id)
            return       
        
        contract = record["contract"]
        symbol = getattr(contract, 'symbol')
        exit_order_ids = [record.get('sl_order_id'), record.get('tp_order_id')]
        side = record.get('side')
        
        while True: 
            await asyncio.sleep(1)
            try: 
                positions = self.ib.positions()
            except Exception:
                positions = []
                
            still_holding = False
            for pos in positions:
                if getattr(pos.contract, "symbol", None) == symbol and pos.position != 0:
                    still_holding = True
                    break
                
            if not still_holding:   
                try: 
                    self.trade_reporter.report_trade_close(trade_id, record)                    
                except Exception as e: 
                    self.logger.exception("🚨 Failed to report trade close for %s exception %s", trade_id, e)      
                    
                fill_price = record.get("fill_price")
                exit_fill_prices = []
                exit_fill_quantities = []
                
                try: 
                    trades = self.ib.trades()
                    for trade in trades: 
                        if getattr(trade, "OrderId", None) in exit_order_ids:
                            for fill in trade.fills: 
                                exit_fill_prices.append(fill.execution.price)
                                exit_fill_quantities.append(fill.execution.shares)
                   
                    # volume weighted average price
                    if exit_fill_prices and exit_fill_quantities:
                        total_quantity = sum(exit_fill_quantities)
                        weighted_sum = sum(p*q for p, q in zip(exit_fill_prices, exit_fill_quantities))
                        avg_exit_price = weighted_sum/total_quantity
                    else: 
                        avg_exit_price = None
                except Exception as e :
                    self.logger.exception("🚨 Failed to fetch exit fills for %s Exception: %s", trade_id, e)   
                    avg_exit_price = None             
                
                try: 
                    if fill_price and avg_exit_price: 
                        pnl_pct = (avg_exit_price - fill_price)/fill_price if side == "BUY" else (fill_price - avg_exit_price)/fill_price
                        
                    if pnl_pct > 0: 
                        self.loss_tracker.add_trade_result(True)
                    else: 
                        self.loss_tracker.add_trade_result(False)
                
                except Exception as e :
                    self.logger.exception("❌ Error computing PnL for %s Exception: %s", trade_id, e)
    
                # Cancel stop-loss and take-profit orders on exit if still active
                try:
                    sl_order_id = record.get("sl_order_id")
                    tp_order_id = record.get("tp_order_id")
    
                    for order_id in [sl_order_id, tp_order_id]:
                        if order_id is not None:
                            ib_orders = self.ib.orders()
                            for ib_order in ib_orders:
                                if getattr(ib_order, "permId", None) == order_id:
                                    self.ib.cancelOrder(ib_order)
                                    self.logger.info("✅ Cancelled order %s on trade exit %s", order_id, trade_id)
                                    break
                except Exception as e:
                    self.logger.exception("❌ Failed to cancel child exit orders for %s: %s", trade_id, e)
                
                try: 
                    del self.active_trades[trade_id]
                except KeyError:
                    pass
                await self.save_state()
                return

                
    async def _periodic_state_save(self):
        try: 
            while True: 
                await asyncio.sleep(self.auto_trade_save_secs)
                await self.save_state()
                
        except Exception as e: 
            self.logger.info("🛑 OrderManager._periodic_state_save exception: %s", e)

    async def _periodic_portfolio_snapshot(self):
        try:
            while True:
                await asyncio.sleep(self.portfolio_snapshot_interval_secs)
                await self.save_portfolio_snapshot()
        except Exception as e:
            self.logger.info(f"🛑 OrderManager._periodic_portfolio_snapshot exception: {e}")

    async def save_portfolio_snapshot(self):
        snapshot_data = {
            "timestamp": datetime.datetime.now().isoformat(),
            "active_trades": {}
        }
        for trade_id, rec in self.active_trades.items():
            snapshot_data["active_trades"][trade_id] = {
                "symbol": getattr(rec.get('contract'), 'symbol', None),
                "qty": rec.get("qty"),
                "side": rec.get("side"),
                "state": rec.get("state"),
                "fill_price": rec.get("fill_price"),
                "sl_price": rec.get("sl_price"),
                "tp_price": rec.get("tp_price"),
                "meta": rec.get("meta")
            }
        try:
            # Append snapshot as new line in JSON Lines format
            with open('reports/portfolio_snapshots.json', 'a') as f:
                f.write(json.dumps(snapshot_data) + '\\n')
            self.logger.info("✅ Portfolio snapshot saved at %s", snapshot_data["timestamp"])
        except Exception as e:
            self.logger.exception("🚨 Failed to save portfolio snapshot: %s", e)
                
    async def save_state(self):
        data = {}
        for trade_id, rec in self.active_trades.items():
            contract = rec.get('contract')
            contract_dict = {
                "symbol": getattr(contract, "symbol", None),
                "secType": getattr(contract, "secType", None),
                "exchange": getattr(contract, "exchange", None),
                "currency": getattr(contract, "currency", None),
                }
            
            data[trade_id] = {
                "contract": contract_dict, 
                "order_id": rec.get("order_id"),
                "qty": rec.get("qty"),
                "side": rec.get("side"),
                "sl_price": rec.get("sl_price"),
                "tp_price": rec.get("tp_price"),
                "meta": rec.get("meta"),
                "state": rec.get("state"),
                "fill_price": rec.get("fill_price")            
                }
            
            try: 
                with open(self.order_manager_state_file, "w") as file: 
                    json.dump(data, file, default=str)
            except Exception: 
                self.logger.exception("🚨 Failed to save order manager state to disk")
        
    async def load_state(self, market_data, ib):
        if not os.path.exists(self.order_manager_state_file):
            self.logger.warning("📂⚠️ Order manager state file '%s' does not exist. Skipping load.", self.order_manager_state_file)
            return
        
        try:
            self.logger.info("📂🔍 Attempting to open order manager state file: '%s'", self.order_manager_state_file)
            with open(self.order_manager_state_file, "r") as file:
                # Try to load JSON data
                data = json.load(file)
            self.logger.info("✅ Successfully loaded order manager state from '%s'", self.order_manager_state_file)
            return data
        except json.decoder.JSONDecodeError as e:
            self.logger.error("📂🚫 State file '%s' exists but is empty or corrupted (invalid JSON). Exception: %s", self.order_manager_state_file, e)
            return
        except Exception as e:
            self.logger.exception("📂🚨 Failed to read order manager state file '%s'. Exception: %s", self.order_manager_state_file, e)
            return
        
        try: 
            positions = ib.positions()
        except: 
            positions = []
        
        
        self.logger.info(positions)
        if not positions:
            self.logger.info("No open positions detected.")
        else:
            # process positions normally
            pass
        
        pos_symbols = {p.contract.symbol: p.position for p in positions}
        print("pos_symbols===>")
        
        print(pos_symbols)
        
        
        print("loaded json===>")
        print(data)
        
        for trade_id, rec in data.items():
            cdict = rec.get('contract', {})
            symbol = cdict.get('symbol')
            print("symbbl inside===> ", symbol)
            
            
            if not symbol: 
                continue
            pos_qty = pos_symbols.get(symbol, 0)  # Fix here
            if pos_qty == 0:
                continue
            contract = market_data.create_stock_contract(symbol, self.exchange, self.currency)
            
            self.active_trades[trade_id] = {
                "contract": contract,
                "order": None,
                "order_id": rec.get("order_id"),
                "qty": abs(int(pos_qty)),
                "side": "BUY" if pos_qty > 0 else "SELL",
                "sl_price": rec.get("sl_price"),
                "tp_price": rec.get("tp_price"),
                "meta": rec.get("meta"),
                "state": "FILLED",
                "fill_price": rec.get("fill_price")
            }
            asyncio.create_task(self._monitor_exit(trade_id))
            
        await self.save_state()
            
    
    def has_active_trades_for_symbol(self, symbol: str) -> bool:
        for rec in self.active_trades.values():
            try: 
                c = rec.get('contract')
                if getattr(c, 'symbol', None) == symbol and rec.get('state') in ('PENDING', 'FILLED'):
                    return True
            except Exception as e: 
                self.logger.info(f"⚠️ Exception in has_active_trades module: {e}")
                
        return False
