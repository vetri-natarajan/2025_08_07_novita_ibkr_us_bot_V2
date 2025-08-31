from ib_async import *
import asyncio


# ==== Date  and time ====
import datetime as dt

class ibkr_connector:
    def __init__(self, acoount_type, ib_instance, ibkr_client_id):
        self.ib = ib_instance
        self.account_type = acoount_type
        self.is_connected = False 
        self.client_id = ibkr_client_id

        
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
                    
                    if self.account_type.upper() == "PAPER":
                        await self.ib.connectAsync('127.0.0.1', 7497, clientId=self.client_id, timeout=4)
                        
                    elif self.account_type.upper() == "LIVE":
                        await self.ib.connectAsync('127.0.0.1', 4003, clientId=self.client_id, timeout=4)
                                                              
                    self.is_connected = True                
                    message = f"\n{current_time} 🔗 ✅ Connected to IB."
                    print(message)
                    #Reporter.send_message(message, self.api_telegram_token, self.api_chat_id)
                    
                    send_first_error = True
                
                await asyncio.sleep(1)  
                
            except Exception as e:
                message = f"❌ Connection error: {e}. 🔄 Reconnecting..."
                print(message)
                if send_first_error == True:
                    self.is_connected = False
                    #Reporter.send_message(message, self.api_telegram_token, self.api_chat_id)
                    send_first_error = False
                await asyncio.sleep(5)  # Wait before reconnecting
    
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