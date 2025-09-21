import pandas as pd
import asyncio
import os
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from ib_async import IB, Stock, util, Index
from tqdm import tqdm
import pytz

class HistoricalDataFetcher:
    def __init__(self, ib: IB, config_dict, watchlist_main_settings, logger, ib_connector, data_dir):
        self.ib = ib
        self.time_zone = config_dict['trading_time_zone']
        self.max_days_per_request = 30  # IB max approx for 1-min+ bars
        self.watchlist_main_settings = watchlist_main_settings
        self.exchange = config_dict['exchange']
        self.exchange_index = config_dict['exchange_index']
        self.currency = config_dict['currency']
        self.spx_symbol = config_dict['spx_symbol']
        self.vix_symbol = config_dict['vix_symbol']
        self.logger = logger
        self.ib_connector = ib_connector
        self.data_dir = data_dir
        
        os.makedirs(data_dir, exist_ok=True)
        
    def parse_duration(self, duration_value: int, duration_unit: str):
        duration_unit = duration_unit.lower()
        if duration_unit in ['year', 'years']:
            return relativedelta(years=duration_value)
        elif duration_unit in ['month', 'months']:
            return relativedelta(months=duration_value)
        elif duration_unit in ['week', 'weeks']:
            return timedelta(weeks=duration_value)
        elif duration_unit in ['day', 'days']:
            return timedelta(days=duration_value)
        elif duration_unit in ['min', 'minute', 'minutes']:
            return timedelta(minutes=duration_value)
        else:
            raise ValueError(f"Unsupported duration unit: {duration_unit}")
            
    def get_start_time(self, end_time: datetime, duration_value: int, duration_unit: str):
        delta = self.parse_duration(duration_value, duration_unit)
        return end_time - delta
    
    def get_filepath(self, symbol, timeframe, start_time, end_time):
        fname = f"{symbol}_{timeframe}_{start_time.strftime('%Y%m%d')}_{end_time.strftime('%Y%m%d')}.parquet"
        return os.path.join(self.data_dir, fname)
    
    def load_data(self, symbol, timeframe, start_time, end_time):
        filepath = self.get_filepath(symbol, timeframe, start_time, end_time)
        if os.path.exists(filepath):
            self.logger.info(f"Loaded cached data from {filepath}")
            return pd.read_parquet(filepath)
        return None
    
    def save_data(self, df, symbol, timeframe, start_time, end_time):
        filepath = self.get_filepath(symbol, timeframe, start_time, end_time)
        df.to_parquet(filepath)
        self.logger.info(f"Saved data to {filepath}")
                                      
    async def fetch_paginated_data(self, symbol_combined: str, symbol: str, timeframe: str, start_time: datetime, end_time: datetime, fetch_index = False) -> pd.DataFrame:
        
        self.logger.info(f"📄 symbol: {symbol}")   
        if fetch_index:
            contract = Index(symbol, self.exchange_index, self.currency)
            await self.ib_connector.ensure_connected()
            qualified = self.ib.qualifyContracts(contract)

        else:
            contract = Stock(symbol, self.exchange, self.currency)
            await self.ib_connector.ensure_connected()
            qualified = self.ib.qualifyContracts(contract)
        self.logger.info(f"📄 Contract: {contract}")    
        self.logger.info(f"✅ Qualified Contract: {qualified[0]}")
        self.logger.info(f"📌 conId: {qualified[0].conId}")
        
        bars = []
        current_end = end_time
        max_delta = timedelta(days=self.max_days_per_request)

        bar_size_map = {
            '1 min': '1 min',
            '5 min': '5 mins',
            '30 min': '30 mins',
            '1 hour': '1 hour',
            '1 day': '1 day',
            '1 week': '1 week',
            '1 month': '1 month'
        }
        bar_size_setting = bar_size_map.get(timeframe.lower(), timeframe)

        # Create list of segments to fetch for progress tracking
        segments = []
        while current_end > start_time:
            current_start = max(start_time, current_end - max_delta)
            segments.append((current_start, current_end))
            current_end = current_start - timedelta(seconds=1)

        self.logger.info(f"Fetching {len(segments)} segments for {symbol_combined} {timeframe} from {start_time} to {end_time}")

        # Progress bar usage
        for current_start, current_end in tqdm(segments, desc=f'Fetching {symbol_combined} {timeframe}', unit='segment'):
            end_str = current_end.strftime('%Y%m%d %H:%M:%S')
            duration_days = (current_end - current_start).days + 1
            duration_str = f"{duration_days} D"
            
            #print(contract, end_str, duration_str, bar_size_setting)
            try:
                await self.ib_connector.ensure_connected()
                partial_bars = await self.ib.reqHistoricalDataAsync(
                    contract,
                    endDateTime=end_str,
                    durationStr=duration_str,
                    barSizeSetting=bar_size_setting,
                    whatToShow='TRADES',
                    useRTH=True,
                    formatDate=1
                )
            except Exception as e:
                self.logger.error(f"Error fetching {symbol_combined} {timeframe} from {current_start} to {current_end}: {e}")
                break
            if not partial_bars:
                self.logger.warning(f"No data returned for {symbol_combined} {timeframe} from {current_start} to {current_end}")
                break
            bars.extend(partial_bars)
            await asyncio.sleep(0.2)  # avoid IB limits
        
        if not bars:
            self.logger.warning(f"No historical data fetched for {symbol_combined} {timeframe}")
            return pd.DataFrame()
        
        df = util.df(bars)
        df['date'] = pd.to_datetime(df['date'])
        df = df.drop_duplicates(subset=['date'])
        df = df.set_index('date')
        df = df.loc[~df.index.duplicated(keep='first')]
        df = df.sort_index()
        
        if df.index.tz is None:
            df.index = df.index.tz_localize(self.time_zone)
        else:
            df.index = df.index.tz_convert(self.time_zone)
       
        
        if start_time.tzinfo is None:
            start_time = start_time.replace(tzinfo=pytz.timezone(self.time_zone))
        if end_time.tzinfo is None:
            end_time = end_time.replace(tzinfo=pytz.timezone(self.time_zone))
            
        print('type', type(df.index ))       
        print('type', type(start_time))
        print('df.index===>', df.index)
        print('start_time===>', start_time)
        print('end_time===>', end_time)
    
        df = df[(df.index >= start_time) & (df.index <= end_time)]
        
        print('df.tail===>', df)

        self.logger.info(f"Completed fetching historical data for {symbol_combined} {timeframe} with {len(df)} records")

        return df

    async def fetch_multiple_timeframes(self, symbol_combined, symbol, start_time, end_time, save=False, load=False):
        self.logger.info("\n\n\nin fetch_multiple_timeframes===>")
        self.logger.info(f"symbol_combined ===> {symbol_combined}")
        self.logger.info(f"start_time ===> {start_time}")
        self.logger.info(f"end_time ===> {end_time}")
        
        timeframes = self.watchlist_main_settings[symbol_combined]['Parsed TF']
        dfs = {}
        for tf in timeframes:
            df = None
            if load:
                df = self.load_data(symbol_combined, tf, start_time, end_time)
            if df is None or df.empty:
                self.logger.info(f" 📄 before: {symbol_combined}") 
                self.logger.info(f" 📄 before: {symbol}") 
                self.logger.info(f" 📄 before: {tf}") 
                self.logger.info(f" 📄 before: {start_time}") 
                self.logger.info(f" 📄 before: {end_time}") 
                
                df = await self.fetch_paginated_data(symbol_combined, symbol, tf, start_time, end_time)
                if save and not df.empty:
                    self.save_data(df, symbol, tf, start_time, end_time)
            dfs[tf] = df
        return dfs, timeframes


    async def fetch_vix_data(self, start_time: datetime, end_time: datetime, save=False, load=False):
        symbol = self.vix_symbol
        timeframe = '1 day'  # Single higher timeframe
        df = None
        if load:
            df = self.load_data(symbol, timeframe, start_time, end_time)
        if df is None or df.empty:
            df = await self.fetch_paginated_data(symbol, symbol, timeframe, start_time, end_time, True)
            #print("df in vix_data===>")
            #print(df)
            if save and not df.empty:
                self.save_data(df, symbol, timeframe, start_time, end_time)
        return df

    async def fetch_spx_data(self, start_time: datetime, end_time: datetime, save=False, load=False):
        symbol = self.spx_symbol
        timeframe = '1 day'  # Single higher timeframe
        df = None
        if load:
            df = self.load_data(symbol, timeframe, start_time, end_time)
            #print("df in spx_data===>")
            #print(df)
        if df is None or df.empty:
            df = await self.fetch_paginated_data(symbol, symbol, timeframe, start_time, end_time, True)
            if save and not df.empty:
                self.save_data(df, symbol, timeframe, start_time, end_time)
        return df