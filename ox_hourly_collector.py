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

# API credentials
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
            mark_price REAL,
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
        
        # Return just the market codes
        return [m['marketCode'] for m in markets_data if 'marketCode' in m]
    
    def get_mark_price(self, market_code):
        """Get the mark price for a market"""
        # Try different approaches to get the price
        
        # Try approach 1: Get latest trades
        url = f'https://api.ox.fun/v3/trades?marketCode={market_code}&limit=1'
        
        response = self.make_request(url)
        
        if response and 'success' in response and response['success'] and 'data' in response:
            try:
                trades_data = response['data']
                if isinstance(trades_data, list) and len(trades_data) > 0:
                    trade = trades_data[0]
                    if 'price' in trade and trade['price'] is not None:
                        price = float(trade['price'])
                        logger.info(f"Got price for {market_code}: {price} (from latest trade)")
                        return price
            except (TypeError, ValueError, IndexError) as e:
                logger.error(f"Error extracting price from trade data: {str(e)}")
        
        # Try approach 2: Get orderbook
        url = f'https://api.ox.fun/v3/orderbook?marketCode={market_code}&depth=1'
        
        response = self.make_request(url)
        
        if response and 'success' in response and response['success'] and 'data' in response:
            try:
                orderbook = response['data']
                if 'asks' in orderbook and isinstance(orderbook['asks'], list) and len(orderbook['asks']) > 0:
                    ask_price = float(orderbook['asks'][0][0])  # First ask price
                    logger.info(f"Got ask price for {market_code}: {ask_price}")
                    return ask_price
                elif 'bids' in orderbook and isinstance(orderbook['bids'], list) and len(orderbook['bids']) > 0:
                    bid_price = float(orderbook['bids'][0][0])  # First bid price
                    logger.info(f"Got bid price for {market_code}: {bid_price}")
                    return bid_price
            except (TypeError, ValueError, IndexError) as e:
                logger.error(f"Error extracting price from orderbook data: {str(e)}")
        
        # Try approach 3: Get market statistics
        url = f'https://api.ox.fun/v3/market-statistics?marketCode={market_code}'
        
        response = self.make_request(url)
        
        if response and 'success' in response and response['success'] and 'data' in response:
            try:
                stats = response['data']
                if isinstance(stats, list) and len(stats) > 0:
                    stat = stats[0]
                    for field in ['last', 'close', 'markPrice', 'indexPrice']:
                        if field in stat and stat[field] is not None:
                            price = float(stat[field])
                            logger.info(f"Got price for {market_code}: {price} (from market statistics, field: {field})")
                            return price
            except (TypeError, ValueError, IndexError) as e:
                logger.error(f"Error extracting price from market statistics: {str(e)}")
        
        logger.warning(f"Could not get current price for {market_code}")
        return None
    
    def fetch_and_store_data(self, market_code):
        """Fetch funding rate and mark price and store them together"""
        # Get funding rate
        url = f'https://api.ox.fun/v3/funding/rates?marketCode={market_code}&limit=1'
        
        funding_response = self.make_request(url)
        
        if not funding_response or 'success' not in funding_response or not funding_response['success']:
            logger.error(f"Failed to fetch funding rate for {market_code}")
            return None
        
        if 'data' not in funding_response or not isinstance(funding_response['data'], list) or len(funding_response['data']) == 0:
            logger.warning(f"No funding rate data for {market_code}")
            return None
            
        funding_data = funding_response['data'][0]
        funding_rate = float(funding_data.get('fundingRate', 0))
        created_at = funding_data.get('createdAt', '')
        
        # Get mark price
        mark_price = self.get_mark_price(market_code)
        
        logger.info(f"Fetched latest funding rate for {market_code}: {funding_rate}, price: {mark_price}")
        
        # Store the data
        self.store_data(market_code, funding_rate, mark_price, created_at)
        
        return {
            'market_code': market_code,
            'funding_rate': funding_rate,
            'mark_price': mark_price,
            'created_at': created_at
        }
    
    def store_data(self, market_code, funding_rate, mark_price, created_at):
        """Store funding rate and mark price in the database"""
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
            (market_code, funding_rate, mark_price, created_at, timestamp, collection_time)
            VALUES (?, ?, ?, ?, ?, ?)
            ''', (market_code, funding_rate, mark_price, created_at, timestamp_str, collection_time))
            
            if cursor.rowcount > 0:
                logger.info(f"Inserted new funding rate with price for {market_code}")
            else:
                logger.info(f"Funding rate for {market_code} already exists")
        
        except Exception as e:
            logger.error(f"Error storing funding rate: {str(e)}")
        
        conn.commit()
        conn.close()
    
    def export_to_csv(self, collection_time):
        """Export the latest data to CSV files with datetime in filename"""
        time_str = collection_time.strftime('%Y%m%d_%H%M%S')
        
        # Create dated directories
        dated_dir = os.path.join(DATA_DIR, collection_time.strftime('%Y-%m-%d'))
        os.makedirs(dated_dir, exist_ok=True)
        
        try:
            conn = sqlite3.connect(self.db_path)
            
            # Export data
            query = f"""
                SELECT market_code, funding_rate, mark_price, timestamp, collection_time 
                FROM funding_rates 
                WHERE date(collection_time) = date('{collection_time.strftime('%Y-%m-%d')}')
                ORDER BY market_code
            """
            
            file_path = os.path.join(dated_dir, f"ox_data_{time_str}.csv")
            data_df = pd.read_sql_query(query, conn)
            data_df.to_csv(file_path, index=False)
            logger.info(f"Exported {len(data_df)} records to {file_path}")
            
            # Create or update a consolidated CSV for the day
            self.update_consolidated_file(collection_time, data_df, dated_dir)
            
            conn.close()
            
        except Exception as e:
            logger.error(f"Error exporting data to CSV: {str(e)}")
    
    def update_consolidated_file(self, collection_time, data_df, dated_dir):
        """Update or create consolidated CSV file for the day"""
        date_str = collection_time.strftime('%Y-%m-%d')
        
        # Consolidated file
        consolidated_file = os.path.join(dated_dir, f"ox_data_{date_str}_consolidated.csv")
        if os.path.exists(consolidated_file):
            existing_data = pd.read_csv(consolidated_file)
            # Remove entries for same markets if they exist
            existing_data = existing_data[~existing_data['market_code'].isin(data_df['market_code'])]
            consolidated_data = pd.concat([existing_data, data_df])
        else:
            consolidated_data = data_df
            
        consolidated_data.to_csv(consolidated_file, index=False)
        logger.info(f"Updated consolidated file with {len(data_df)} new records")
    
    def collect_data(self):
        """Collect data for all markets"""
        collection_time = datetime.now()
        logger.info(f"Starting data collection at {collection_time}")
        
        # Fetch all markets
        all_markets = self.fetch_markets()
        futures_markets = [m for m in all_markets if '-SWAP-LIN' in m]
        
        logger.info(f"Collecting data for {len(futures_markets)} futures markets")
        
        success_count = 0
        
        # Process each market
        for i, market_code in enumerate(futures_markets):
            logger.info(f"Processing {i+1}/{len(futures_markets)}: {market_code}")
            
            try:
                # Fetch and store data
                result = self.fetch_and_store_data(market_code)
                if result:
                    success_count += 1
                
                # Add a small delay to avoid rate limiting
                time.sleep(0.5)
                
            except Exception as e:
                logger.error(f"Error processing {market_code}: {str(e)}")
        
        logger.info(f"Data collection complete. Successfully processed {success_count} markets")
        
        # Export data to CSV
        self.export_to_csv(collection_time)
        
        return {
            'collection_time': collection_time,
            'markets_processed': len(futures_markets),
            'success_count': success_count
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
    logger.info(f"Successfully processed: {results['success_count']}")
    
    logger.info("OX Hourly Data Collector completed successfully")
