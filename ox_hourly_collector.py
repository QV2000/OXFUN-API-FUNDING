import requests
import time
import pandas as pd
from datetime import datetime
import os
import sqlite3
import json
import logging
import csv

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

# Define output directories - adjusted for GitHub Actions environment
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
        
        # Markets table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS markets (
            market_code TEXT PRIMARY KEY,
            name TEXT,
            base TEXT,
            counter TEXT,
            type TEXT,
            tick_size TEXT,
            min_size TEXT,
            listed_at TEXT,
            last_updated TEXT,
            metadata TEXT
        )
        ''')
        
        # Funding rates table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS funding_rates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            market_code TEXT,
            funding_rate REAL,
            price REAL,
            created_at TEXT,
            timestamp DATETIME,
            collection_time DATETIME,
            UNIQUE(market_code, created_at)
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
        
        # Store markets in database
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        for market in markets_data:
            if 'marketCode' in market:
                cursor.execute('''
                INSERT OR REPLACE INTO markets
                (market_code, name, base, counter, type, tick_size, min_size, listed_at, last_updated, metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    market.get('marketCode', ''),
                    market.get('name', ''),
                    market.get('base', ''),
                    market.get('counter', ''),
                    market.get('type', ''),
                    market.get('tickSize', ''),
                    market.get('minSize', ''),
                    market.get('listedAt', ''),
                    market.get('lastUpdatedAt', ''),
                    json.dumps(market)
                ))
        
        conn.commit()
        conn.close()
        
        # Return just the market codes
        return [m['marketCode'] for m in markets_data if 'marketCode' in m]
    
    def get_current_price(self, market_code):
        """Get the current price for a market using the 60s candle"""
        # Try different timeframes in order of preference
        timeframes = ["60s", "300s", "900s"]
        
        for timeframe in timeframes:
            url = f'https://api.ox.fun/v3/candles?marketCode={market_code}&timeframe={timeframe}&limit=1'
            
            response = self.make_request(url)
            
            if response and 'success' in response and response['success'] and 'data' in response:
                if isinstance(response['data'], list) and len(response['data']) > 0:
                    candle = response['data'][0]
                    # Get the closing price as the current price
                    if 'close' in candle:
                        price = self.safe_float(candle, 'close', 0)
                        logger.info(f"Got current price for {market_code}: {price} (using {timeframe} timeframe)")
                        return price
        
        logger.warning(f"Could not get current price for {market_code}")
        return None
    
    def fetch_and_store_funding_with_price(self, market_code):
        """Fetch the latest funding rate and current price together"""
        url = f'https://api.ox.fun/v3/funding/rates?marketCode={market_code}&limit=1'
        
        response = self.make_request(url)
        
        if not response or 'success' not in response or not response['success']:
            logger.error(f"Failed to fetch funding rate for {market_code}")
            return None
        
        if 'data' not in response or not isinstance(response['data'], list) or len(response['data']) == 0:
            logger.warning(f"No funding rate data for {market_code}")
            return None
            
        latest_rate = response['data'][0]
        funding_rate = float(latest_rate.get('fundingRate', 0))
        created_at = latest_rate.get('createdAt', '')
        
        # Get the current price
        price = self.get_current_price(market_code)
        
        logger.info(f"Fetched latest funding rate for {market_code}: {funding_rate}, price: {price}")
        
        # Store the funding rate and price together
        self.store_funding_with_price(market_code, funding_rate, price, created_at)
        
        return {
            'market_code': market_code,
            'funding_rate': funding_rate,
            'price': price,
            'created_at': created_at
        }
    
    def store_funding_with_price(self, market_code, funding_rate, price, created_at):
        """Store a funding rate with its price in the database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
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
            (market_code, funding_rate, price, created_at, timestamp, collection_time)
            VALUES (?, ?, ?, ?, ?, ?)
            ''', (market_code, funding_rate, price, created_at, timestamp_str, collection_time))
            
            if cursor.rowcount > 0:
                logger.info(f"Inserted new funding rate with price for {market_code}")
            else:
                logger.info(f"Funding rate for {market_code} already exists")
        
        except Exception as e:
            logger.error(f"Error storing funding rate: {str(e)}")
        
        conn.commit()
        conn.close()
    
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
            
            # Export funding rates with prices
            funding_query = f"""
                SELECT market_code, funding_rate, price, timestamp, collection_time 
                FROM funding_rates 
                WHERE date(collection_time) = date('{collection_time.strftime('%Y-%m-%d')}')
                ORDER BY market_code
            """
            
            funding_file = os.path.join(dated_dir, f"ox_funding_rates_{time_str}.csv")
            funding_df = pd.read_sql_query(funding_query, conn)
            funding_df.to_csv(funding_file, index=False)
            logger.info(f"Exported {len(funding_df)} funding rates with prices to {funding_file}")
            
            # Create or update a consolidated CSV for the day
            self.update_consolidated_files(collection_time, funding_df, dated_dir)
            
            conn.close()
            
        except Exception as e:
            logger.error(f"Error exporting data to CSV: {str(e)}")
    
    def update_consolidated_files(self, collection_time, funding_df, dated_dir):
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
    
    def collect_data(self):
        """Collect latest data for all markets"""
        collection_time = datetime.now()
        logger.info(f"Starting data collection at {collection_time}")
        
        # Fetch all markets
        all_markets = self.fetch_markets()
        futures_markets = [m for m in all_markets if '-SWAP-LIN' in m]
        
        logger.info(f"Collecting data for {len(futures_markets)} futures markets")
        
        funding_count = 0
        
        # Process each market
        for i, market_code in enumerate(futures_markets):
            logger.info(f"Processing {i+1}/{len(futures_markets)}: {market_code}")
            
            try:
                # Fetch funding rate with price
                result = self.fetch_and_store_funding_with_price(market_code)
                if result:
                    funding_count += 1
                
                # Add a small delay to avoid rate limiting
                time.sleep(0.5)
                
            except Exception as e:
                logger.error(f"Error processing {market_code}: {str(e)}")
        
        logger.info(f"Data collection complete. Collected {funding_count} funding rates with prices")
        
        # Export data to CSV
        self.export_to_csv(collection_time)
        
        return {
            'collection_time': collection_time,
            'markets_processed': len(futures_markets),
            'funding_rates_collected': funding_count
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
    
    logger.info("OX Hourly Data Collector completed successfully")
