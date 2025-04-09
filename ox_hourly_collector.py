import requests
import time
import pandas as pd
from datetime import datetime
import os
import sqlite3
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("ox_funding_collector.log")
    ]
)
logger = logging.getLogger("ox_funding_collector")

# API credentials - for GitHub Actions, these should be set as repository secrets
API_KEY = os.environ.get("OX_API_KEY", "your_api_key")
SECRET_KEY = os.environ.get("OX_SECRET_KEY", "your_secret_key")

# Define output directories
DATA_DIR = os.environ.get("DATA_DIR", "data")
DB_PATH = os.path.join(DATA_DIR, "ox_funding_data.db")

# Create data directory if it doesn't exist
os.makedirs(DATA_DIR, exist_ok=True)

class OxFundingCollector:
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
        self.max_retries = 3
        
        # Initialize database
        self.init_database()
    
    def init_database(self):
        """Initialize SQLite database with funding rates table"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
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
            elif response.status_code == 429:  # Rate limit
                logger.warning(f"Rate limited. Waiting before retry.")
                time.sleep(delay * 5)
                return self.make_request(url, retry_count + 1)
            else:
                logger.error(f"Request failed with status code {response.status_code}: {response.text}")
                if retry_count < self.max_retries - 1:
                    time.sleep(delay * 2)
                    return self.make_request(url, retry_count + 1)
                return None
                
        except Exception as e:
            logger.error(f"Request error: {str(e)}")
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
        
        # Filter for futures markets
        futures_markets = [m['marketCode'] for m in markets_data if 'marketCode' in m and '-SWAP-LIN' in m['marketCode']]
        logger.info(f"Found {len(futures_markets)} futures markets")
        
        return futures_markets
    
    def fetch_and_store_funding_rate(self, market_code):
        """Fetch the latest funding rate for a market"""
        url = f'https://api.ox.fun/v3/funding/rates?marketCode={market_code}&limit=1'
        
        response = self.make_request(url)
        
        if not response or 'success' not in response or not response['success']:
            logger.error(f"Failed to fetch funding rate for {market_code}")
            return False
        
        if 'data' not in response or not isinstance(response['data'], list) or len(response['data']) == 0:
            logger.warning(f"No funding rate data for {market_code}")
            return False
            
        funding_data = response['data'][0]
        funding_rate = float(funding_data.get('fundingRate', 0))
        created_at = funding_data.get('createdAt', '')
        
        logger.info(f"Fetched funding rate for {market_code}: {funding_rate}")
        
        # Store the funding rate
        return self.store_funding_rate(market_code, funding_rate, created_at)
    
    def store_funding_rate(self, market_code, funding_rate, created_at):
        """Store a funding rate in the database"""
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
                    timestamp_str = collection_time
            else:
                timestamp_str = collection_time
            
            cursor.execute('''
            INSERT OR IGNORE INTO funding_rates
            (market_code, funding_rate, created_at, timestamp, collection_time)
            VALUES (?, ?, ?, ?, ?)
            ''', (market_code, funding_rate, created_at, timestamp_str, collection_time))
            
            success = cursor.rowcount > 0
            if success:
                logger.info(f"Inserted new funding rate for {market_code}")
            else:
                logger.info(f"Funding rate for {market_code} already exists")
            
            conn.commit()
            conn.close()
            return success
            
        except Exception as e:
            logger.error(f"Error storing funding rate: {str(e)}")
            conn.rollback()
            conn.close()
            return False
    
    def export_to_csv(self, collection_time):
        """Export the latest data to CSV files"""
        time_str = collection_time.strftime('%Y%m%d_%H%M%S')
        
        # Create dated directories
        dated_dir = os.path.join(DATA_DIR, collection_time.strftime('%Y-%m-%d'))
        os.makedirs(dated_dir, exist_ok=True)
        
        try:
            conn = sqlite3.connect(self.db_path)
            
            # Export funding rates for this collection
            query = f"""
                SELECT market_code, funding_rate, timestamp, collection_time 
                FROM funding_rates 
                WHERE date(collection_time) = date('{collection_time.strftime('%Y-%m-%d')}')
                ORDER BY market_code
            """
            
            file_path = os.path.join(dated_dir, f"ox_funding_rates_{time_str}.csv")
            funding_df = pd.read_sql_query(query, conn)
            funding_df.to_csv(file_path, index=False)
            logger.info(f"Exported {len(funding_df)} funding rates to {file_path}")
            
            # Create or update consolidated file for the day
            consolidated_file = os.path.join(dated_dir, f"ox_funding_rates_{collection_time.strftime('%Y-%m-%d')}_consolidated.csv")
            
            if os.path.exists(consolidated_file):
                existing_df = pd.read_csv(consolidated_file)
                # Remove entries for same markets if they exist
                existing_df = existing_df[~existing_df['market_code'].isin(funding_df['market_code'])]
                consolidated_df = pd.concat([existing_df, funding_df])
            else:
                consolidated_df = funding_df
                
            consolidated_df.to_csv(consolidated_file, index=False)
            logger.info(f"Updated consolidated file with {len(funding_df)} new records")
            
            conn.close()
            
        except Exception as e:
            logger.error(f"Error exporting data to CSV: {str(e)}")
    
    def collect_funding_rates(self):
        """Collect funding rates for all futures markets"""
        collection_time = datetime.now()
        logger.info(f"Starting funding rate collection at {collection_time}")
        
        # Fetch markets
        futures_markets = self.fetch_markets()
        
        if not futures_markets:
            logger.error("No futures markets found. Aborting collection.")
            return {
                'collection_time': collection_time,
                'markets_processed': 0,
                'funding_rates_collected': 0
            }
        
        # Collect funding rates
        success_count = 0
        
        for i, market_code in enumerate(futures_markets):
            logger.info(f"Processing {i+1}/{len(futures_markets)}: {market_code}")
            
            try:
                if self.fetch_and_store_funding_rate(market_code):
                    success_count += 1
                
                # Small delay to avoid rate limiting
                time.sleep(0.5)
                
            except Exception as e:
                logger.error(f"Error processing {market_code}: {str(e)}")
        
        logger.info(f"Collection complete. Collected {success_count} funding rates.")
        
        # Export to CSV
        self.export_to_csv(collection_time)
        
        return {
            'collection_time': collection_time,
            'markets_processed': len(futures_markets),
            'funding_rates_collected': success_count
        }

# Main execution
if __name__ == "__main__":
    logger.info("Starting OX Funding Rate Collector")
    
    # Check for API credentials
    if API_KEY == "your_api_key" or SECRET_KEY == "your_secret_key":
        logger.error("API credentials not configured. Please set OX_API_KEY and OX_SECRET_KEY environment variables.")
        exit(1)
    
    collector = OxFundingCollector()
    results = collector.collect_funding_rates()
    
    logger.info("Collection Summary:")
    logger.info(f"Collection time: {results['collection_time']}")
    logger.info(f"Markets processed: {results['markets_processed']}")
    logger.info(f"Funding rates collected: {results['funding_rates_collected']}")
    
    logger.info("OX Funding Rate Collector completed successfully")
