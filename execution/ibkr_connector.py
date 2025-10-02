from ib_async import *
import asyncio


# ==== Date  and time ====
import datetime as dt


class ibkr_connector:
    def __init__(self, tws_or_gateway, account_type, ib_instance, ibkr_client_id, logger):
        self.ib = ib_instance
        self.tws_or_gateway = tws_or_gateway
        self.account_type = account_type
        self.is_connected = False 
        self.client_id = ibkr_client_id
        self.logger = logger
        self.port_id = None
        self.host_id = '127.0.0.1'
        self._async_connected = asyncio.Event()
        self._async_connect_lock = asyncio.Lock()

        self.logger.info(f'len of tws{len(self.tws_or_gateway)} {self.tws_or_gateway}')
        self.logger.info(
        "\n"
        "====================================\n"
        "🔌 IBKR Connection Info\n"
        "====================================\n"
        f"🖥️ TWS/Gateway: {tws_or_gateway}\n"
        f"👤 Account Type: {account_type}\n"
        f"🆔 Client ID: {ibkr_client_id}\n"
        "===================================="
        )

        
    async def connect(self):
        """
        Asynchronously connect to IB and handle reconnections

        Returns
        -------
        None.

        """
     
        send_first_error = True
        while True:
            try:
                current_time = dt.datetime.now()
                #print("\n", current_time)
                #print(f"Is IB connected? ===> {self.ib.isConnected()}")                    
                if not self.ib.isConnected():
                    self.is_connected = False  
                    self._async_connected.clear()
                    
                    if self.account_type.upper() == "PAPER":
                        self.logger.info("insdie paper")
                        if self.tws_or_gateway == "TWS":
                            self.logger.info("insdie tws")
                            self.port_id = 7497
                        if self.tws_or_gateway == "GATEWAY":
                            self.port_id = 4002        
                        else: 
                            self.logger.info("pimpilaka pi....")
                    elif self.account_type.upper() == "LIVE":
                        if self.tws_or_gateway == "TWS":
                            self.port_id = 7496
                        if self.tws_or_gateway == "GATEWAY":
                            self.port_id = 4001
                            
                    await self.ib.connectAsync(self.host_id, self.port_id, clientId=self.client_id, timeout=4)
                                        
                    self.is_connected = True 
                    self._async_connected.set()
                    message = f"\n{current_time} 🔗 ✅ Connected to IB."
                    self.logger.info(message)
                    #Reporter.send_message(message, self.api_telegram_token, self.api_chat_id)
                    
                    send_first_error = True
                
                await asyncio.sleep(1)  
                
            except Exception as e:
                message = f"❌ Connection error: {e}. 🔄 Reconnecting..."
                self.logger.info(message)
                if send_first_error == True:
                    self.is_connected = False
                    #Reporter.send_message(message, self.api_telegram_token, self.api_chat_id)
                    send_first_error = False
                await asyncio.sleep(5)  # Wait before reconnecting
    
    async def ensure_connected(self):
        if self.ib.isConnected():
            if not self._async_connected.is_set():
                self._async_connected.set()
            return
        async with self._async_connect_lock:
            if self.ib.isConnected():
                self._async_connected.set()
                return

        await self._async_connected.wait()
            
        
        
    
    def create_stock_contract(self, symbol: str, exchange: str, currency: str):
     return Stock(symbol, exchange, currency) 
                
    async def disconnect(self):
        """
         Disconnect from IBKR API

        Returns
        -------
        None.

        """
        self.ib.disconnect()