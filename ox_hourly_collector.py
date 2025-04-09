import requests
import time
import pandas as pd
from datetime import datetime
import os
import sqlite3
import json
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("ox_hourly_collector.log")
    ]
)
logger = logging.getLogger("ox_hourly_collector")

# API credentials - for GitHub Actions, these should be set as repository secrets
API_KEY = os.environ.get("OX_API_KEY", "your_api_key")
SECRET_KEY = os.environ.get("OX_SECRET_KEY", "your_secret_key")

# Define output directories
DATA_DIR = os.environ.get("DATA_DIR", "data")
DB_PATH = os.path.join(DATA_DIR, "ox_hourly_data.db")

# Create data directory if it doesn't exist
os.makedirs(DATA_DIR, exist_ok=True)

class OxHourlyCollector:
    def __init__(self, api_key=API_KEY, secret_key=SECRET_KEY, db_path=DB_PATH):
        self.api_key = api_key
        self.secret_key = secret_key
        self.db_path = db_path
        self.headers = {
            'Content-Type': 'application/json',
            'X-API-KEY': self.api_key,
            'X-SECRET-KEY': self.secret_key
        }
        
        self.base_delay = 1.0
        self.rate_limit_delay = 60
        self.max_retries = 5
        
        # Initialize database
        self.init_database()
    
    def init_database(self):
        """Initialize SQLite database with necessary tables"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Funding rates table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS funding_rates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            market_code TEXT,
            funding_rate REAL,
            created_at TEXT,
            timestamp DATETIME,
            collection_time DATETIME,
            UNIQUE(market_code, created_at)
        )
        ''')
        
        # OHLC candles table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS candles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            market_code TEXT,
            timeframe TEXT,
            timestamp DATETIME,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume REAL,
            time_ms TEXT,
            collection_time DATETIME,
            UNIQUE(market_code, timeframe, time_ms)
        )
        ''')
        
        conn.commit()
        conn.close()
        logger.info(f"Database initialized at {self.db_path}")
    
    def make_request(self, url, retry_count=0):
        """Make API request with retry logic"""
        if retry_count >= self.max_retries:
            logger.warning(f"Max retries ({self.max_retries}) exceeded for URL: {url}")
            return None
        
        try:
            delay = self.base_delay * (1 + retry_count * 0.5)
            time.sleep(delay)
            
            response = requests.get(url, headers=self.headers)
            
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429:
                logger.warning(f"Rate limited on retry {retry_count+1}. Waiting {self.rate_limit_delay} seconds...")
                time.sleep(self.rate_limit_delay)
                return self.make_request(url, retry_count + 1)
            else:
                logger.error(f"Request failed with status code {response.status_code}: {response.text}")
                if retry_count < self.max_retries - 1:
                    time.sleep(delay * 2)
                    return self.make_request(url, retry_count + 1)
                return None
                
        except Exception as e:
            logger.error(f"Request error on retry {retry_count+1}: {str(e)}")
            if retry_count < self.max_retries - 1:
                time.sleep(self.base_delay * (2 ** retry_count))
                return self.make_request(url, retry_count + 1)
            return None
    
    def fetch_markets(self):
        """Fetch all available markets"""
        logger.info("Fetching all markets...")
        
        url = 'https://api.ox.fun/v3/markets'
        response = self.make_request(url)
        
        if not response or 'success' not in response or not response['success'] or 'data' not in response:
            logger.error("Failed to fetch markets.")
            return []
        
        markets_data = response['data']
        logger.info(f"Found {len(markets_data)} markets")
        
        # Return just the market codes
        return [m['marketCode'] for m in markets_data if 'marketCode' in m]
    
    def fetch_latest_funding_rate(self, market_code):
        """Fetch only the latest funding rate for a market"""
        url = f'https://api.ox.fun/v3/funding/rates?marketCode={market_code}&limit=1'
        
        response = self.make_request(url)
        
        if not response or 'success' not in response or not response['success']:
            logger.error(f"Failed to fetch funding rate for {market_code}")
            return None
        
        if 'data' not in response or not isinstance(response['data'], list) or len(response['data']) == 0:
            logger.warning(f"No funding rate data for {market_code}")
            return None
            
        latest_rate = response['data'][0]
        logger.info(f"Fetched latest funding rate for {market_code}: {latest_rate.get('fundingRate', 'N/A')}")
        
        # Store the funding rate
        self.store_funding_rate(latest_rate)
        
        return latest_rate
    
    def store_funding_rate(self, entry):
        """Store a single funding rate entry in the database"""
        if not entry:
            return
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            market_code = entry.get('marketCode', '')
            funding_rate = float(entry.get('fundingRate', 0))
            created_at = entry.get('createdAt', '')
            collection_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            if created_at:
                try:
                    timestamp = datetime.fromtimestamp(int(created_at) / 1000)
                    timestamp_str = timestamp.strftime('%Y-%m-%d %H:%M:%S')
                except (ValueError, OSError):
                    logger.warning(f"Invalid timestamp: {created_at}, using current time")
                    timestamp_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            else:
                timestamp_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            cursor.execute('''
            INSERT OR IGNORE INTO funding_rates
            (market_code, funding_rate, created_at, timestamp, collection_time)
            VALUES (?, ?, ?, ?, ?)
            ''', (market_code, funding_rate, created_at, timestamp_str, collection_time))
            
            if cursor.rowcount > 0:
                logger.info(f"Inserted new funding rate for {market_code}")
            else:
                logger.info(f"Funding rate for {market_code} already exists")
        
        except Exception as e:
            logger.error(f"Error storing funding rate: {str(e)}")
        
        conn.commit()
        conn.close()

    def fetch_candles_page(self, market_code, timeframe='10800s', limit=1):
        """Fetch a page of candles for a specific market and timeframe
           Using logic from the original script"""
        url = f'https://api.ox.fun/v3/candles?marketCode={market_code}&timeframe={timeframe}&limit={limit}'
        logger.info(f"Trying candle endpoint: {url}")
        
        response = self.make_request(url)
        
        if response and 'success' in response and response['success'] and 'data' in response:
            candles = response['data']
            if isinstance(candles, list) and len(candles) > 0:
                logger.info(f"Successfully fetched {len(candles)} {timeframe} candles for {market_code}")
                return candles
            else:
                logger.warning(f"Empty candle list returned")
        
        logger.error(f"Failed to fetch {timeframe} candles for {market_code}")
        return []
    
    def identify_timestamp_field(self, sample_candle):
        """Identify the timestamp field in a candle
           Using logic from the original script"""
        if 'openedAt' in sample_candle and sample_candle['openedAt']:
            try:
                value = sample_candle['openedAt']
                if isinstance(value, (int, float)) or (isinstance(value, str) and value.isdigit()):
                    num_value = int(value)
                    if num_value > 1000000000000:
                        logger.info(f"Using 'openedAt' as timestamp field")
                        return 'openedAt'
            except (ValueError, TypeError):
                pass

        for field in ['time', 't', 'timestamp', 'closeTime', 'date', 'createdAt']:
            if field in sample_candle:
                try:
                    value = sample_candle[field]
                    if isinstance(value, (int, float)) or (isinstance(value, str) and value.isdigit()):
                        num_value = int(value)
                        if num_value > 1000000000000:
                            logger.info(f"Identified timestamp field: '{field}'")
                            return field
                except (ValueError, TypeError):
                    continue
        
        logger.warning(f"Could not identify timestamp field. Using 'openedAt' as fallback")
        return 'openedAt'

    def fetch_latest_candle(self, market_code):
        """Attempt to fetch the latest candle using various timeframes"""
        # Try different timeframes - ordered by preference
        timeframes = ["10800s", "3600s", "900s", "300s", "60s"]
        
        for timeframe in timeframes:
            candles = self.fetch_candles_page(market_code, timeframe=timeframe, limit=1)
            if candles and len(candles) > 0:
                logger.info(f"Successfully fetched candle with timeframe {timeframe}")
                self.store_candles(market_code, timeframe, candles)
                return candles[0], timeframe
        
        logger.warning(f"Could not fetch candles for {market_code} with any timeframe")
        return None, None
    
    def store_candles(self, market_code, timeframe, candles):
        """Store candles in the database
           Using logic from the original script"""
        if not candles:
            return
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        inserted_count = 0
        collection_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        time_field = 'openedAt'
        if candles and len(candles) > 0:
            identified_field = self.identify_timestamp_field(candles[0])
            if identified_field:
                time_field = identified_field
        
        for candle in candles:
            try:
                time_ms = None
                if time_field and time_field in candle:
                    time_ms = str(candle[time_field])
                
                if not time_ms:
                    for field in ['openedAt', 'time', 't', 'timestamp', 'closeTime', 'date', 'createdAt']:
                        if field in candle and candle[field]:
                            try:
                                time_ms = str(candle[field])
                                break
                            except (ValueError, TypeError):
                                continue
                
                if not time_ms:
                    time_ms = str(int(time.time() * 1000))
                    logger.warning(f"Using current time as fallback timestamp for a candle")
                
                try:
                    timestamp = datetime.fromtimestamp(int(time_ms) / 1000)
                    timestamp_str = timestamp.strftime('%Y-%m-%d %H:%M:%S')
                except (ValueError, TypeError, OSError):
                    logger.warning(f"Invalid timestamp {time_ms}, using current time")
                    timestamp = datetime.now()
                    timestamp_str = timestamp.strftime('%Y-%m-%d %H:%M:%S')
                
                open_val = self.safe_float(candle, 'open', 0)
                high_val = self.safe_float(candle, 'high', 0)
                low_val = self.safe_float(candle, 'low', 0)
                close_val = self.safe_float(candle, 'close', 0)
                volume_val = self.safe_float(candle, 'volume', 0)
                
                cursor.execute('''
                INSERT OR IGNORE INTO candles
                (market_code, timeframe, timestamp, open, high, low, close, volume, time_ms, collection_time)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    market_code,
                    timeframe,
                    timestamp_str,
                    open_val,
                    high_val,
                    low_val,
                    close_val,
                    volume_val,
                    time_ms,
                    collection_time
                ))
                
                if cursor.rowcount > 0:
                    inserted_count += 1
                
            except Exception as e:
                logger.error(f"Error storing candle: {str(e)}")
        
        conn.commit()
        conn.close()
        
        logger.info(f"Inserted {inserted_count} new candles for {market_code} ({timeframe})")
    
    def safe_float(self, data, key, default=0):
        """Safely convert a value to float"""
        try:
            if key in data and data[key] is not None:
                return float(data[key])
        except (ValueError, TypeError):
            logger.debug(f"Could not convert {key} value to float: {data.get(key)}")
        return default
    
    def export_to_csv(self, collection_time):
        """Export the latest data to CSV files with datetime in filename"""
        time_str = collection_time.strftime('%Y%m%d_%H%M%S')
        
        # Create dated directories
        dated_dir = os.path.join(DATA_DIR, collection_time.strftime('%Y-%m-%d'))
        os.makedirs(dated_dir, exist_ok=True)
        
        try:
            conn = sqlite3.connect(self.db_path)
            
            # Export funding rates
            funding_query = f"""
                SELECT market_code, funding_rate, timestamp, collection_time 
                FROM funding_rates 
                WHERE date(collection_time) = date('{collection_time.strftime('%Y-%m-%d')}')
                ORDER BY market_code
            """
            
            funding_file = os.path.join(dated_dir, f"ox_funding_rates_{time_str}.csv")
            funding_df = pd.read_sql_query(funding_query, conn)
            funding_df.to_csv(funding_file, index=False)
            logger.info(f"Exported {len(funding_df)} funding rates to {funding_file}")
            
            # Export candles
            candles_query = f"""
                SELECT market_code, timeframe, timestamp, open, high, low, close, volume, collection_time
                FROM candles 
                WHERE date(collection_time) = date('{collection_time.strftime('%Y-%m-%d')}')
                ORDER BY market_code
            """
            
            candles_file = os.path.join(dated_dir, f"ox_candles_{time_str}.csv")
            candles_df = pd.read_sql_query(candles_query, conn)
            candles_df.to_csv(candles_file, index=False)
            logger.info(f"Exported {len(candles_df)} candles to {candles_file}")
            
            # Create or update consolidated files for the day
            self.update_consolidated_files(collection_time, funding_df, candles_df, dated_dir)
            
            conn.close()
            
        except Exception as e:
            logger.error(f"Error exporting data to CSV: {str(e)}")
    
    def update_consolidated_files(self, collection_time, funding_df, candles_df, dated_dir):
        """Update or create consolidated CSV files for the day"""
        date_str = collection_time.strftime('%Y-%m-%d')
        
        # Consolidated funding rates file
        funding_consolidated = os.path.join(dated_dir, f"ox_funding_rates_{date_str}_consolidated.csv")
        if os.path.exists(funding_consolidated):
            existing_funding = pd.read_csv(funding_consolidated)
            # Remove entries for same markets if they exist
            existing_funding = existing_funding[~existing_funding['market_code'].isin(funding_df['market_code'])]
            consolidated_funding = pd.concat([existing_funding, funding_df])
        else:
            consolidated_funding = funding_df
            
        consolidated_funding.to_csv(funding_consolidated, index=False)
        logger.info(f"Updated consolidated funding rates file with {len(funding_df)} new records")
        
        # Consolidated candles file
        candles_consolidated = os.path.join(dated_dir, f"ox_candles_{date_str}_consolidated.csv")
        if os.path.exists(candles_consolidated):
            existing_candles = pd.read_csv(candles_consolidated)
            # Remove entries for same markets if they exist
            existing_candles = existing_candles[~(
                (existing_candles['market_code'].isin(candles_df['market_code'])) & 
                (existing_candles['timeframe'].isin(candles_df['timeframe']))
            )]
            consolidated_candles = pd.concat([existing_candles, candles_df])
        else:
            consolidated_candles = candles_df
            
        consolidated_candles.to_csv(candles_consolidated, index=False)
        logger.info(f"Updated consolidated candles file with {len(candles_df)} new records")
    
    def collect_data(self):
        """Collect latest data for all markets"""
        collection_time = datetime.now()
        logger.info(f"Starting data collection at {collection_time}")
        
        # Fetch all markets
        all_markets = self.fetch_markets()
        futures_markets = [m for m in all_markets if '-SWAP-LIN' in m]
        
        logger.info(f"Collecting data for {len(futures_markets)} futures markets")
        
        funding_count = 0
        candle_count = 0
        
        # Process each market
        for i, market_code in enumerate(futures_markets):
            logger.info(f"Processing {i+1}/{len(futures_markets)}: {market_code}")
            
            try:
                # Fetch funding rate
                funding_rate = self.fetch_latest_funding_rate(market_code)
                if funding_rate:
                    funding_count += 1
                
                # Fetch candle - using the approach from the original script
                candle, timeframe = self.fetch_latest_candle(market_code)
                if candle:
                    candle_count += 1
                    logger.info(f"Got candle for {market_code}: O:{self.safe_float(candle, 'open')}, H:{self.safe_float(candle, 'high')}, L:{self.safe_float(candle, 'low')}, C:{self.safe_float(candle, 'close')}")
                
                # Add a small delay to avoid rate limiting
                time.sleep(0.5)
                
            except Exception as e:
                logger.error(f"Error processing {market_code}: {str(e)}")
        
        logger.info(f"Data collection complete. Collected {funding_count} funding rates and {candle_count} candles")
        
        # Export data to CSV
        self.export_to_csv(collection_time)
        
        return {
            'collection_time': collection_time,
            'markets_processed': len(futures_markets),
            'funding_rates_collected': funding_count,
            'candles_collected': candle_count
        }

# Main execution
if __name__ == "__main__":
    logger.info("Starting OX Hourly Data Collector")
    
    # Check for API credentials
    if API_KEY == "your_api_key" or SECRET_KEY == "your_secret_key":
        logger.error("API credentials not configured. Please set OX_API_KEY and OX_SECRET_KEY environment variables.")
        exit(1)
    
    collector = OxHourlyCollector()
    results = collector.collect_data()
    
    logger.info("Collection Summary:")
    logger.info(f"Collection time: {results['collection_time']}")
    logger.info(f"Markets processed: {results['markets_processed']}")
    logger.info(f"Funding rates collected: {results['funding_rates_collected']}")
    logger.info(f"Candles collected: {results['candles_collected']}")
    
    logger.info("OX Hourly Data Collector completed successfully")
