from ib_async import IB, MarketOrder, StopOrder, LimitOrder
from typing import Dict, Any
import asyncio
import json
import os
import datetime
from data.market_data import MarketData
from indicators.vwap import calculate_vwap
from pathlib import Path

from risk_management.fixed_sl_tp import compute_fixed_sl_tp
from risk_management.atr_based_sl_tp import compute_atr_sl_tp
from risk_management.dynamic_sl_tp import compute_dynamic_sl_swing
from risk_management.hedge_sl_tp import compute_hedge_exit_trade

from indicators.atr import calculate_atr


from utils.ensure_directory import ensure_directory



class order_manager_class:
    def __init__(self, ib : IB, 
                 trade_reporter, 
                 loss_tracker, 
                 order_manager_state_file, 
                 trade_timeout_secs,
                 auto_trade_save_secs, 
                 config_dict,
                 watchlist_main_settings,
                 market_data,
                 ib_connector,
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
        self.config_dict = config_dict
        self.watchlist_main_settings = watchlist_main_settings
        self.market_data = market_data  # Store streaming market data object
        self.ib_connector  = ib_connector
        
        self.intraday_scalping_exit_time = config_dict["intraday_scalping_exit_time"]
        self.order_state_folder = Path(order_manager_state_file).parent
        dirctory = ensure_directory(self.order_state_folder, self.logger)
        
        self.logger.info("✅ Order manager initialized successfully")


    async def place_market_entry_with_bracket(self, 
                                              symbol_combined,
                                              symbol,
                                              contract, 
                                              qty: int, 
                                              side: str, 
                                              df_LTF,
                                              sl_price: float = None,
                                              tp_price: float = None, 
                                              meta : Dict = None, 
                                              last_price: float = None, 
                                              order_testing: bool = False,
                                              special_exit: bool = False, 
                                              ):
        if qty <= 0: 
            self.logger.warning("⚠️ Quantity <= 0, skipping order placement")
            return None            
        
        if order_testing:
            qty = 1
            
        parent = MarketOrder('BUY', qty)
        parent.orderId = self.ib.client.getReqId()
        parent.transmit = False
        
        # Stop loss
        stopLoss = StopOrder('SELL', qty, sl_price)
        stopLoss.orderId = self.ib.client.getReqId()
        stopLoss.parentId = parent.orderId
        stopLoss.transmit = False
        
        # Take profit
        takeProfit = LimitOrder('SELL', qty, tp_price)
        takeProfit.orderId = self.ib.client.getReqId()
        takeProfit.parentId = parent.orderId
        takeProfit.transmit = True
        
        
        parent_trade = self.ib.placeOrder(contract, parent)
        sl_trade = self.ib.placeOrder(contract, stopLoss)
        tp_trade = self.ib.placeOrder(contract, takeProfit)
        
        # Place bracket order
        trades = []
        trades.append(parent_trade)
        trades.append(sl_trade)
        trades.append(tp_trade)        
        
        
        order_id = getattr(parent_trade.orderStatus, "orderId", None)
        sl_order_id = getattr(sl_trade.orderStatus, "orderId", None)
        tp_order_id = getattr(tp_trade.orderStatus, "orderId", None)

        self.logger.info(
            "\n"
            "====================================\n"
            "✅ Bracket Order Placed\n"
            "====================================\n"
            f"📝 Order ID: {order_id}\n"
            f"📈 Symbol: [{symbol_combined}]\n"
            f"🔢 Quantity: {qty}\n"
            f"🛑 Stoploss: {sl_price} (ID: {sl_order_id})\n"
            f"🎯 Target: {tp_price} (ID: {tp_order_id})\n"
            f"🔗 Parent Order ID: {order_id}\n"
            "===================================="
        )


        self.logger.info(f'📝 parent_order_id: {order_id} | 🛑 sl_order_id: {sl_order_id} | 🎯 tp_order_id: {tp_order_id}')        
        trade_id = f"{symbol}-{order_id}"
        record = {
                "symbol_combined": symbol_combined,
                "symbol": symbol,
                "contract": contract,
                "order": parent,
                "sl_order": stopLoss,
                "tp_order": takeProfit,
                "order_id": order_id,
                "sl_order_id": sl_order_id,
                "tp_order_id": tp_order_id,
                "qty": qty,
                "side": side.upper(),
                "sl_price": sl_price,
                "tp_price": tp_price,
                "meta": meta or {},
                "state": "PENDING",
                "fill_price": None,
                "special_exit" : special_exit,
                "entry_time": datetime.datetime.utcnow()  # Store entry time here
                }
        
        self.active_trades[trade_id] = record
        
        
        asyncio.create_task(self._monitor_fill_and_attach_children(trade_id, last_price, order_testing, df_LTF))
        await self.save_state()
        return trade_id
        
        


    async def _monitor_fill_and_attach_children(self, trade_id: str, last_price: float, order_testing: bool, df_LTF):
        record = self.active_trades.get(trade_id)
        if not record:
            self.logger.warning("⚠️ No active trade record found for %s; nothing to monitor.", trade_id)
            return
        
        symbol_combined = record["symbol_combined"]
        symbol = record["symbol"]
        contract = record["contract"]
        order_id = record.get("order_id")
        special_exit = record.get("special_exit")
        side = record.get("side")

        # 1. Wait for parent order fill or timeout
        for i in range(self.trade_timeout_secs * 2):
            await asyncio.sleep(0.5)
            await self.ib_connector.ensure_connected()
            trades = self.ib.trades()
            parent_trade = None
            for t in trades:
                if getattr(t.order, "orderId", None) == order_id:
                    parent_trade = t
                    break
            if parent_trade and parent_trade.isDone():
                record["state"] = "FILLED"
                fill_price = None
                try:
                    if parent_trade.fills:
                        fill_price = parent_trade.fills[-1].execution.price
                except Exception as e:
                    self.logger.info(f"⚠️ Exception in monitor fill: {e}, passing.")
                record["fill_price"] = fill_price
                self.logger.info("✅ Parent order filled for %s at price %s", trade_id, fill_price)
                break

        else:
            # Timeout expired without fill
            record["state"] = "FAILED"
            self.logger.warning("⏳❌ Parent order for %s did not fill within timeout; marked FAILED", trade_id)
            await self.save_state()
            return

        exit_method = self.watchlist_main_settings[symbol]['Exit']
        qty = record.get("qty")
        entry_price = record.get("fill_price") or record.get("entry_price")
        entry_time = record.get("entry_time") or datetime.datetime.utcnow()

        try:
            self.trade_reporter.report_trade_open(trade_id, record)
        except Exception as e:
            self.logger.exception("🚨 Failed to report trade open for %s exception %s", trade_id, e)

        asyncio.create_task(self._monitor_exit(trade_id))
        await self.save_state()
        
        sl_input = self.watchlist_main_settings[symbol_combined]['SL']
        tp_input = self.watchlist_main_settings[symbol_combined]['TP']
        old_sl = record.get("sl_price")
        old_tp = record.get("tp_price")
        old_sl = round(old_sl)
        old_tp = round(old_tp)
        

        if exit_method == "E1":
            sl_price_new, tp_price_new = compute_fixed_sl_tp(entry_price, sl_input, tp_input)
            sl_price_new = round(sl_price_new)
            tp_price_new = round(tp_price_new)       
            
        elif exit_method == "E2":
            try:
                atr_val_df = calculate_atr(df_LTF, 5)
                atr_val = float(atr_val_df.iloc[-1])
            except Exception:
                atr_val = None
            if atr_val is not None:
                sl_price_new, tp_price_new = compute_atr_sl_tp(entry_price, atr_val, sl_input, tp_input)
                sl_price_new = round(sl_price_new)
                tp_price_new = round(tp_price_new)         
                
            if sl_price_new != old_sl:
                self.logger.info(f"🔄 Changing Stoploss: 🛑 {old_sl} ➡️ {sl_price_new}")
                await self._update_order(trade_id, contract, sl=True, tp=False, auto_close = False, new_price = sl_price_new, qty = qty, side = side)
                record["sl_price"] = sl_price_new        
      
            if tp_price_new != old_tp:                       
                self.logger.info(f"🔄 Changing Target: 🛑 {old_tp} ➡️ {tp_price_new}")
                await self._update_order(trade_id, contract, sl=False, tp=True, auto_close = False, new_price = tp_price_new, qty = qty, side = side)
                record["tp_price"] = tp_price_new     
                
                
        if exit_method in ['E3', 'E4']:
            # Continuous monitoring loop without timeout for dynamic SL/TP and qty
            while True:
                await asyncio.sleep(0.5)                
                
                current_price = self.get_live_price(symbol)
                
                if not order_testing:                    
                    vwap = self.get_vwap(symbol, ltf='1min')
                    if vwap is None:
                        self.logger.warning(f"Skipping VWAP update for {symbol} due to insufficient data")
                        continue
                else: 
                    vwap = last_price*0.995 #testing mode assuming the vwap

                qty = record.get("qty")  # dynamic qty

                if exit_method == 'E3':
                    signals = compute_dynamic_sl_swing(entry_price, current_price, record)
                    new_sl = signals["stop_loss"]
                    take_70_profit = signals["take_70_profit"]
                    exit_remaining = signals["exit_remaining"]
                    sl_triggered = signals["sl_triggered"]
                    updated_qty = signals["updated_qty"]
                    
                    new_sl = round(new_sl)
                    
                    old_sl = record.get("sl_price")
                    old_sl = round(old_sl)
                    if new_sl != old_sl:
                        self.logger.info(f"🔄 Changing Stoploss: 🛑 {old_sl} ➡️ {new_sl}")
                        await self._update_order(trade_id, contract, sl=True, tp=False, auto_close = False, new_price = new_sl, qty = qty, side = side)
                        record["sl_price"] = new_sl
                        
                    if take_70_profit and not record.get("profit_taken", False):
                        self.logger.info(f"🎯💰 Taking partial profit at {take_70_profit}")
                        await self.take_partial_profit(trade_id, percent=70)
                        record["profit_taken"] = True
                        record["qty"] = updated_qty

                    '''
                    if exit_remaining:
                        await self.execute_trade_exit(trade_id, reason="final_take_profit")
                        return
                    
                    if sl_triggered:
                        await self.execute_trade_exit(trade_id, reason="stop_loss_triggered")
                        return
                    '''


                elif exit_method == 'E4':
                    signals = compute_hedge_exit_trade(entry_price, current_price, vwap, entry_time, datetime.datetime.utcnow())
                    new_sl = signals["stop_loss"]
                    sl_triggered = signals["sl_triggered"]
                    tp_price = signals["tp_price"]
                    take_profit = signals["take_profit"]
                    auto_close = signals["auto_close"]
                    
                    new_sl = round(new_sl)
                    tp_price = round(tp_price)

                    old_sl = record.get("sl_price")
                    old_sl = round(old_sl)
                    if new_sl != old_sl:
                        self.logger.info(f"🔄 Changing Stoploss: 🛑 {old_sl} ➡️ {new_sl}")
                        await self._update_order(trade_id, contract, sl=True, tp=False, auto_close = False, new_price = new_sl, qty = qty, side = side)
                        record["sl_price"] = new_sl

                    old_tp = record.get("tp_price")
                    old_tp = round(old_tp)
                    if tp_price != old_tp:                       
                        self.logger.info(f"🔄 Changing Target: 🛑 {old_tp} ➡️ {tp_price}")
                        await self._update_order(trade_id, contract, sl=False, tp=True, auto_close = False, new_price = tp_price, qty = qty, side = side)
                        record["tp_price"] = tp_price
                        
                    '''
                    if take_profit:
                        self.logger.info("🎯 Take-Profit price reached... 🚀 Initiating Take-Profit")
                        await self.execute_trade_exit(trade_id, sl=False, tp=True, auto_close=False, reason="take_profit_hit")
                        return
                    '''
                    
                    if auto_close:
                        self.logger.info("⏳ Auto-close days reached... 💼 Initiating Exit")
                        await self._update_order(trade_id, contract, sl=False, tp=False, auto_close = True, new_price = None, qty = qty, side = side)
                        return
                    '''
                    if sl_triggered:
                        self.logger.info("🛑💥 Stoploss level reached... Triggering stop loss orders...")
                        await self.execute_trade_exit(trade_id, sl=True, tp=False, auto_close=False, reason="stop_loss_triggered")
                        return
                    '''

            asyncio.create_task(self._monitor_exit(trade_id))
            await self.save_state()
            return

    async def _monitor_exit(self, trade_id: str):
        record = self.active_trades.get(trade_id)
        if not record:
            self.logger.warning("⚠️ No active trade record found for %s; nothing to monitor.", trade_id)
            return
    
        contract = record["contract"]
        symbol_combined = record["symbol_combined"]
        symbol = getattr(contract, 'symbol')
        
        exit_order_ids = [record.get('sl_order_id'), record.get('tp_order_id')]
        side = record.get('side')
    
        while True:
            await asyncio.sleep(1)
            self.logger.info(f"🔄 Monitoring trade for [{symbol}] order_id: {trade_id}")

    
            # Check for intraday timed exit for scalping mode
            mode = self.watchlist_main_settings.get(symbol_combined, {}).get("Mode").upper()
            if mode == "SCALPING":
                current_time = datetime.datetime.now().time()
                exit_hour, exit_min = self.intraday_scalping_exit_time.split(':')
                exit_time = datetime.time(int(exit_hour), int(exit_min))  # 15:00 or 3 PM local time
                if current_time >= exit_time:
                    self.logger.info(f"🕒 Intraday time exit triggered for scalping trade {trade_id} at {current_time}")
                    await self.execute_trade_exit(trade_id, reason="intraday_time_exit")
                    return
    
            try:
                await self.ib_connector.ensure_connected()
                positions = self.ib.positions()
            except Exception:
                positions = []
    
            still_holding = False
            for pos in positions:
                if getattr(pos.contract, "symbol", None) == symbol and pos.position != 0:
                    still_holding = True
                    break
        
            if still_holding:
                #self.logger.info(f"⏳ Still holding trade [{symbol}] order_id: {trade_id}")
                pass
                
            if not still_holding:
                self.logger.info(f"📤 Trade exited for [{symbol}] order_id: {trade_id}")
                try:
                    self.trade_reporter.report_trade_close(trade_id, record)
                except Exception as e:
                    self.logger.exception("🚨 Failed to report trade close for %s exception %s", trade_id, e)
    
                fill_price = record.get("fill_price")
                exit_fill_prices = []
                exit_fill_quantities = []
                
                self.logger.info(f"exit_order_ids====================> {exit_order_ids}")
                try:
                    await self.ib_connector.ensure_connected()
                    trades = self.ib.trades()
                    for trade in trades:
                        self.logger.info(f"\n\n\ntrade===> {trade}")
                        #temp_id = getattr(trade.fills.OrderId, "OrderId", None)
                        temp_id = trade.order.orderId
                        #self.logger.info(f"\nexit_order_ids====================> {exit_order_ids}")
                        #self.logger.info(f"\ntempid outsdie====================> {temp_id}")
                        if temp_id in exit_order_ids:
                            #self.logger.info(f"\ntempid inside====================> {temp_id}")
                            for fill in trade.fills:
                                exit_fill_prices.append(fill.execution.price)
                                exit_fill_quantities.append(fill.execution.shares)
    
                    # Calculate volume weighted average exit price
                    if exit_fill_prices and exit_fill_quantities:
                        total_quantity = sum(exit_fill_quantities)
                        weighted_sum = sum(p * q for p, q in zip(exit_fill_prices, exit_fill_quantities))
                        avg_exit_price = weighted_sum / total_quantity
                    else:
                        avg_exit_price = None
                        
                    self.logger.info(f"💵 Average exit price [{symbol}] order_id: {trade_id} {avg_exit_price}")
    
                except Exception as e:
                    self.logger.exception("🚨 Failed to fetch exit fills for %s Exception: %s", trade_id, e)
                    avg_exit_price = None
    
                try:
                    if fill_price and avg_exit_price:
                        pnl_pct = ((avg_exit_price - fill_price) / fill_price) if side == "BUY" else ((fill_price - avg_exit_price) / fill_price)
                        if pnl_pct > 0:
                            self.loss_tracker.add_trade_result(True)
                        else:
                            self.loss_tracker.add_trade_result(False)
                        self.logger.info(f"➗ PnL percent [{symbol}] order_id: {trade_id} {pnl_pct}")

                except Exception as e:
                    self.logger.exception("❌ Error computing PnL for %s Exception: %s", trade_id, e)
                
                try:
                    del self.active_trades[trade_id]
                except KeyError:
                    pass
    
                await self.save_state()
                return



    async def _update_order(self, trade_id, contract, sl, tp, auto_close, new_price, qty, side):
        record = self.active_trades.get(trade_id)
        if not record:
            return
        try:
            ib_order = None
            new_price = round(new_price)
            if sl == True:
                existing_order_id = record.get("sl_order_id")
                ib_order = record.get("sl_order")
                ib_order.auxPrice = new_price
                record["sl_price"] = new_price
            
            if tp == True: 
                existing_order_id = record.get("tp_order_id")
                ib_order = record.get("tp_order")        
                ib_order.lmtPrice = new_price      
                record["tp_price"] = new_price

            if auto_close == True: 
                existing_order_id = record.get("tp_order_id")
                ib_order = record.get("tp_order")        
                ib_order.orderType  = "MKT"                
            
            ib_order.totalQuantity = qty
            ib_order.transmit =True
            if existing_order_id is not None:                
                if ib_order:
                    await self.ib_connector.ensure_connected()
                    self.ib.placeOrder(contract, ib_order)
                    self.logger.info(f"✅ Modified existing STOP order in place for {trade_id} at {new_price} qty {qty}")
                    return

            # Fallback: cancel & replace
            #await self._cancel_and_replace_stop_order(trade_id, contract, new_price, qty, side)
        except Exception as e:
            self.logger.exception(f"❌ Failed to update stop order for {trade_id}: {e}")
            

    async def _cancel_and_replace_stop_order(self, trade_id, contract, new_price, qty, side):
        record = self.active_trades.get(trade_id)
        if not record:
            return
        order_id_key = "sl_order_id"
        existing_order_id = record.get(order_id_key)
        if existing_order_id is not None:
            for ib_order in self.ib.orders():
                if getattr(ib_order, "permId", None) == existing_order_id:
                    await self.ib_connector.ensure_connected()
                    self.ib.cancelOrder(ib_order)
                    self.logger.info(f"✅ Cancelled existing STOP order {existing_order_id} for {trade_id}")
                    break
        sl_side = "SELL" if side == "BUY" else "BUY"
        stop_order = StopOrder(sl_side, qty, new_price)
        sl_trade = self.ib.placeOrder(contract, stop_order)
        record[order_id_key] = getattr(sl_trade.orderStatus, "permId", None)
        self.logger.info(f"✅ Placed new STOP order for {trade_id} at {new_price} qty {qty}")

    async def _update_limit_order(self, trade_id, contract, order_type, new_price, qty, side):
        record = self.active_trades.get(trade_id)
        if not record:
            return
        try:
            order_id_key = f"{order_type}_order_id"
            existing_order_id = record.get(order_id_key)
            if existing_order_id is not None:
                ib_order = None
                await self.ib_connector.ensure_connected()
                for o in self.ib.orders():
                    if getattr(o, "permId", None) == existing_order_id:
                        ib_order = o
                        break
                if ib_order:
                    # Modify in place
                    ib_order.totalQuantity = qty
                    # For LimitOrder, price stored in 'lmtPrice'
                    ib_order.lmtPrice = new_price
                    await self.ib_connector.ensure_connected()
                    self.ib.placeOrder(contract, ib_order)
                    self.logger.info(f"✅ Modified existing LIMIT order in place for {trade_id} at {new_price} qty {qty}")
                    return

            # Fallback: cancel & replace
            await self._cancel_and_replace_limit_order(trade_id, contract, new_price, qty, side)
        except Exception as e:
            self.logger.exception(f"❌ Failed to update limit order for {trade_id}: {e}")

    async def _cancel_and_replace_limit_order(self, trade_id, contract, new_price, qty, side):
        record = self.active_trades.get(trade_id)
        if not record:
            return
        order_id_key = "tp_order_id"
        existing_order_id = record.get(order_id_key)
        if existing_order_id is not None:
            await self.ib_connector.ensure_connected()
            for ib_order in self.ib.orders():
                if getattr(ib_order, "permId", None) == existing_order_id:
                    self.ib.cancelOrder(ib_order)
                    self.logger.info(f"✅ Cancelled existing LIMIT order {existing_order_id} for {trade_id}")
                    break
        tp_side = "SELL" if side == "BUY" else "BUY"
        limit_order = LimitOrder(tp_side, qty, new_price)
        await self.ib_connector.ensure_connected()
        tp_trade = self.ib.placeOrder(contract, limit_order)
        record[order_id_key] = getattr(tp_trade.orderStatus, "permId", None)
        self.logger.info(f"✅ Placed new LIMIT order for {trade_id} at {new_price} qty {qty}")



    def _calculate_partial_qty(self, record):
        original_qty = record.get("qty", 0)
        profit_taken = record.get("profit_taken", False)
        if profit_taken:
            # assume 70% partial exit
            return int(original_qty * 0.3)
        return original_qty

    # Stub: Replace with actual data fetch method
    def get_live_price(self, symbol):
        
        ticker = self.market_data.tickers.get(symbol)
        live_price = None
        if ticker is not None: 
            live_price = ticker.last
        return live_price
    

    async def take_partial_profit(self, trade_id, percent: float):
        """
        Execute partial profit taking by selling the specified percent of current qty.
    
        Assumes long position only.
    
        Args:
        - trade_id (str): Trade identifier
        - percent (float): Percentage of current qty to close, e.g. 70 for 70%
        """
        record = self.active_trades.get(trade_id)
        if not record:
            self.logger.warning(f"⚠️ Trade record not found for partial profit {trade_id}")
            return
    
        qty = record.get("qty", 0)
        if qty <= 0:
            self.logger.warning(f"⚠️ No quantity to take partial profit on for {trade_id}")
            return
    
        partial_qty = int(qty * percent / 100)
        if partial_qty == 0:
            self.logger.warning(f"⚠️ Partial qty calculated as zero for {trade_id}")
            return
    
        contract = record.get("contract")
        side = record.get("side")
        if side != "BUY":
            self.logger.error(f"Partial profit logic currently supports long positions only for {trade_id}")
            return
    
        sell_side = "SELL"
        try:
            # Place market order to close partial quantity
            order = MarketOrder(sell_side, partial_qty)
            await self.ib_connector.ensure_connected()
            trade = self.ib.placeOrder(contract, order)
            await trade.filledEvent  # Wait for fill event (optional)
            self.logger.info(f"✅ Partial profit taken for {trade_id}, closed {partial_qty} shares out of {qty}")
    
            # Update record qty
            new_qty = qty - partial_qty
            record["qty"] = new_qty
            
            #update_profit_order_qty
            tp_order = record.get("tp_order")
            tp_order.totalQuantity = new_qty
            tp_order.transmit = True
            await self.ib_connector.ensure_connected()
            self.ib.placeOrder(contract, tp_order)       
            
            await self.save_state()
        except Exception as e:
            self.logger.exception(f"❌ Partial profit failed for {trade_id} exception: {e}")

    def get_vwap(self, symbol_combined, symbol, ltf='1min') -> float:
        """
        Gets VWAP of previous completed candle in given timeframe using streaming data.

        Returns None if data insufficient.
        """
        
        parsed_tf = self.watchlist_main_settings[symbol_combined]['Parsed TF']
        ltf = parsed_tf[2]
        df = self.market_data.get_latest(symbol, ltf)
        if df is None or df.empty or len(df) < 2:
            self.logger.warning(f"VWAP data insufficient for {symbol} {ltf}")
            return None

        vwaps = calculate_vwap(df)
        # Return VWAP of the previous (complete) candle (second last index)
        return vwaps.iloc[-2]

    async def execute_trade_exit(self, trade_id, sl=False, tp=False, auto_close = False, reason=None):
        """
        Fully exit trade: cancel orders, close position, clean active_trades etc.
        """
        self.logger.info(f"Executing full trade exit for {trade_id}, reason: {reason}")
        record = self.active_trades.get(trade_id)
        if not record:
            return
        try:
            sl_order_id = record.get("sl_order_id")
            tp_order_id = record.get("tp_order_id")
            sl_order = record.get("sl_order")
            tp_order = record.get("tp_order")
            
            if sl == True: 
                order = sl_order
                sl.order
            
            for oid in filter(None, [sl_order_id, tp_order_id]):
                await self.ib_connector.ensure_connected()
                for ib_order in self.ib.orders():
                    if getattr(ib_order, "orderId", None) == oid:
                        await self.ib_connector.ensure_connected()
                        self.ib.cancelOrder(ib_order)
                        self.logger.info(f"✅ Cancelled order {oid} during exit for {trade_id}")
            qty = record.get("qty", 0)
            if qty > 0:
                contract = record.get("contract")
                side = record.get("side")
                # Opposite side to close position
                close_side = "SELL" if side == "BUY" else "BUY"
                close_order = MarketOrder(close_side, qty)
                await self.ib_connector.ensure_connected()
                close_trade = self.ib.placeOrder(contract, close_order)
                await close_trade.filledEvent  # Optionally wait for fill
                self.logger.info(f"✅ Placed market order to close {qty} shares for {trade_id} side {close_side}")
            del self.active_trades[trade_id]
        except Exception as e:
            self.logger.exception(f"❌ Error during execute_trade_exit for {trade_id}: {e}")
        await self.save_state()



               
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
        timestamp = datetime.datetime.now().isoformat()
        snapshot_data = {
            "timestamp": timestamp,
            "active_trades": {}
        }
    
        # Build JSON snapshot
        lines_to_write = []  # plain-text lines, one per trade
        for trade_id, rec in self.active_trades.items():
            contract = rec.get("contract")
            symbol_combined = rec["symbol_combined"]
            symbol = getattr(contract, "symbol", None)
            entry = {
                "symbol_combined": symbol_combined,
                "symbol": symbol,
                "qty": rec.get("qty"),
                "side": rec.get("side"),
                "state": rec.get("state"),
                "fill_price": rec.get("fill_price"),
                "sl_price": rec.get("sl_price"),
                "tp_price": rec.get("tp_price"),
                "meta": rec.get("meta"),
            }
            snapshot_data["active_trades"][trade_id] = entry
    
            # Construct a concise, single-line text record per trade
            # Ensure it's one line by replacing newlines in meta if needed
            meta_text = str(entry["meta"]).replace("\n", " ") if entry["meta"] is not None else ""
            line = (
                f"{timestamp} | id={trade_id} | sym={symbol} | side={entry['side']} "
                f"| qty={entry['qty']} | state={entry['state']} | fill={entry['fill_price']} "
                f"| sl={entry['sl_price']} | tp={entry['tp_price']} | meta={meta_text}"
            )
            lines_to_write.append(line)
    
        try:
            Path("reports").mkdir(parents=True, exist_ok=True)
    
            # JSON Lines append (unchanged)
            with open("reports/portfolio_snapshots.json", "a", encoding="utf-8") as f_jsonl:
                f_jsonl.write(json.dumps(snapshot_data, default=str) + "\n")
    
            # Plain text append: write line-by-line
            # Option A: one line per trade (lines_to_write)
            with open("reports/portfolio_snapshots_readable.txt", "a", encoding="utf-8") as f_txt:
                for line in lines_to_write:
                    f_txt.write(line + "\n")
    
            # Option B (alternative): one summarized line per snapshot
            # Uncomment to use instead of per-trade lines
            # summary = (
            #     f"{timestamp} | trades={len(self.active_trades)} | ids={[tid for tid in self.active_trades.keys()]}"
            # )
            # with open("reports/portfolio_snapshots.txt", "a", encoding="utf-8") as f_txt:
            #     f_txt.write(summary + "\n")
    
            self.logger.info("✅ Portfolio snapshot saved at %s", timestamp)
        except Exception as e:
            self.logger.exception("🚨 Failed to save portfolio snapshot: %s", e)
                  
    async def save_state(self):
        data = {}
        for trade_id, rec in self.active_trades.items():
            contract = rec.get('contract')
            symbol_combined =  getattr(rec, "symbol_combined", None)
            contract_dict = {
                "symbol_combined": symbol_combined,
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
            symbol_combined = cdict.get('symbol_combined')
            print("symbbl inside===> ", symbol_combined)
            
            
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
           
   
    def has_active_trades_for_symbol(self, symbol_combined: str, symbol: str) -> bool:
        for rec in self.active_trades.values():
            try: 
                c = rec.get('contract')
                symbol_combined_rec = rec.get('symbol_combined')
                if getattr(c, 'symbol', None) == symbol and rec.get('state') in ('PENDING', 'FILLED') and symbol_combined == symbol_combined_rec:
                    return True
            except Exception as e: 
                self.logger.info(f"⚠️ Exception in has_active_trades module: {e}")
                
        return False

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