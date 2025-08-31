# -*- coding: utf-8 -*-
"""
Created on Tue Aug 26 09:35:43 2025

@author: Vetriselvan
"""

import asyncio
from strategies.scalping_strategy import check_HTF_conditions, check_MTF_conditions, check_LTF_conditions
from execution.position_sizer import compute_qty
from indicators.atr import calculate_atr
from risk_management.atr_based_sl_tp import compute_atr_sl_tp
from risk_management.fixed_sl_tp import compute_fixed_sl_tp
import logging

logger = logging.getLogger(__name__)

async def process_trading_signals(
    symbol,
    df_HTF,
    df_MTF,
    df_LTF,
    market_data,
    order_manager,
    cfg,
    account_value,
    vix,
    logger,
    watchlist_main_settings,
    ta_settings,
    max_look_back,
    trading_units,
    vix_threshold,
    vix_reduction_factor,
    skip_on_high_vix,
    exit_method,
    exit_fixed_sl_pct,
    exit_fixed_tp_pct,
    exit_atr_k_sl,
    exit_atr_k_tp,
):
    try:
        # Check higher timeframe conditions
        okHTF = check_HTF_conditions(symbol, watchlist_main_settings, ta_settings, max_look_back, df_HTF, logger)
        if not okHTF:
            logger.info(f"⏸️ HTF conditions not satisfied for {symbol}")
            return False

        # Check medium timeframe conditions
        okMTF = check_MTF_conditions(symbol, watchlist_main_settings, ta_settings, max_look_back, df_MTF, logger)
        if not okMTF:
            logger.info(f"⏸️ MTF conditions not satisfied for {symbol}")
            return False

        # Check lower timeframe conditions
        okLTF = check_LTF_conditions(symbol, watchlist_main_settings, ta_settings, max_look_back, df_LTF, df_HTF, logger)
        if not okLTF:
            logger.info(f"⏸️ LTF conditions not satisfied for {symbol}")
            return False

        # Compute last price and quantity
        last_price = df_LTF['close'].iloc[-1]
        qty = compute_qty(
            account_value,
            trading_units,
            last_price,
            vix,
            vix_threshold,
            vix_reduction_factor,
            skip_on_high_vix,
        )
        if qty <= 0:
            logger.info(f"⚠️ Quantity computed as 0 for {symbol}")
            return False

        # Compute stop loss and take profit
        if exit_method.upper() == "FIXED":
            sl_price, tp_price = compute_fixed_sl_tp(last_price, exit_fixed_sl_pct, exit_fixed_tp_pct)
        elif exit_method.upper() == "ATR":
            try:
                atr_val_df = calculate_atr(df_LTF, max_look_back * 2)
                atr_val = float(atr_val_df.iloc[-1])
            except Exception:
                atr_val = None

            if atr_val is not None:
                sl_price, tp_price = compute_atr_sl_tp(last_price, atr_val, exit_atr_k_sl, exit_atr_k_tp)
            else:
                sl_price, tp_price = compute_fixed_sl_tp(last_price, exit_fixed_sl_pct, exit_fixed_tp_pct)
        else:
            sl_price, tp_price = compute_fixed_sl_tp(last_price, exit_fixed_sl_pct, exit_fixed_tp_pct)

        # Ensure contract is available
        contract = None
        if market_data:
            contract = market_data._subscribed.get(symbol, {}).get('contract')
        if contract is None and hasattr(order_manager, 'get_contract'):
            contract = order_manager.get_contract(symbol)
        if contract is None:
            logger.warning(f"No contract found for {symbol}, skipping order")
            return False

        # Avoid duplicate trades
        if order_manager.has_active_trades_for_symbol(symbol):
            logger.info(f"Active trade already exists for {symbol}, skipping order placement.")
            return False

        # Place the trade
        meta = {'signal': okLTF, 'symbol': symbol}
        trade_id = await order_manager.place_market_entry_with_bracket(
            contract,
            qty,
            'BUY',
            sl_price,
            tp_price,
            meta
        )
        if trade_id:
            logger.info(f"✅ Placed trade {trade_id} for {symbol} qty {qty}")
            return True
        return False

    except Exception as e:
        logger.exception(f"Error processing trading signals for {symbol}: {e}")
        return False
