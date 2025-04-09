import requests
import time
import pandas as pd
from datetime import datetime
import os
import sqlite3
import logging
import random
import sys

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
        
        # Improved rate limiting configuration
        self.base_delay = 2.0  # Increased initial delay
        self.max_retries = 5   # Increased max retries
        self.backoff_factor = 2.0  # Exponential backoff factor
        self.jitter = 0.3  # Random jitter to add variation to retry timing
        
        # Counters for rate limit tracking
        self.rate_limit_hits = 0
        self.consecutive_rate_limits = 0
        self.max_rate_limit_pause = 120  # Maximum pause in seconds
        
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
        
        # Create table to track failed requests
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS failed_requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            market_code TEXT,
            request_time DATETIME,
            error_message TEXT,
            UNIQUE(market_code, request_time)
        )
        ''')
        
        conn.commit()
        conn.close()
        logger.info(f"Database initialized at {self.db_path}")
    
    def calculate_backoff_delay(self, retry_count):
        """Calculate backoff delay with jitter"""
        delay = self.base_delay * (self.backoff_factor ** retry_count)
        # Add jitter to avoid thundering herd problem
        jitter_amount = delay * self.jitter
        delay += random.uniform(-jitter_amount, jitter_amount)
        return max(self.base_delay, delay)  # Ensure at least base delay
    
    def adaptive_rate_limit_pause(self):
        """Implement adaptive pausing when hitting rate limits frequently"""
        self.consecutive_rate_limits += 1
        
        # If we've hit multiple rate limits in a row, take a longer pause
        if self.consecutive_rate_limits > 2:
            pause_time = min(
                10 * (2 ** (self.consecutive_rate_limits - 2)), 
                self.max_rate_limit_pause
            )
            logger.warning(f"Multiple consecutive rate limits detected. Pausing for {pause_time} seconds")
            time.sleep(pause_time)
            return True
        return False
    
    def make_request(self, url, retry_count=0):
        """Make API request with improved retry logic and adaptive rate limiting"""
        if retry_count >= self.max_retries:
            logger.warning(f"Max retries ({self.max_retries}) exceeded for URL: {url}")
            return None
        
        try:
            # Apply delay for all requests except the first one
            if retry_count > 0:
                delay = self.calculate_backoff_delay(retry_count)
                logger.info(f"Retry {retry_count}/{self.max_retries}. Waiting {delay:.2f} seconds before retry.")
                time.sleep(delay)
            
            response = requests.get(url, headers=self.headers, timeout=30)
            
            if response.status_code == 200:
                # Reset consecutive rate limit counter on success
                self.consecutive_rate_limits = 0
                return response.json()
            elif response.status_code == 429:  # Rate limit
                self.rate_limit_hits += 1
                logger.warning(f"Rate limited ({self.rate_limit_hits} total). Waiting before retry.")
                
                # Check for rate limit headers
                retry_after = response.headers.get('Retry-After')
                if retry_after:
                    wait_time = int(retry_after) + random.uniform(1, 3)
                    logger.info(f"Server requested wait time of {retry_after} seconds. Waiting {wait_time} seconds.")
                    time.sleep(wait_time)
                else:
                    # Apply adaptive rate limiting
                    self.adaptive_rate_limit_pause()
                    delay = self.calculate_backoff_delay(retry_count + 2)  # Higher backoff for rate limits
                    logger.info(f"No Retry-After header. Waiting {delay:.2f} seconds.")
                    time.sleep(delay)
                
                return self.make_request(url, retry_count + 1)
            else:
                logger.error(f"Request failed with status code {response.status_code}: {response.text}")
                if retry_count < self.max_retries - 1:
                    delay = self.calculate_backoff_delay(retry_count)
                    time.sleep(delay)
                    return self.make_request(url, retry_count + 1)
                return None
                
        except requests.exceptions.Timeout:
            logger.error(f"Request timed out for URL: {url}")
            if retry_count < self.max_retries - 1:
                delay = self.calculate_backoff_delay(retry_count)
                time.sleep(delay)
                return self.make_request(url, retry_count + 1)
            return None
        except Exception as e:
            logger.error(f"Request error: {str(e)}")
            if retry_count < self.max_retries - 1:
                delay = self.calculate_backoff_delay(retry_count)
                time.sleep(delay)
                return self.make_request(url, retry_count + 1)
            return None
    
    def log_failed_request(self, market_code, error_message):
        """Log failed request to database for later retry"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            request_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            cursor.execute('''
            INSERT OR REPLACE INTO failed_requests
            (market_code, request_time, error_message)
            VALUES (?, ?, ?)
            ''', (market_code, request_time, error_message))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            logger.error(f"Error logging failed request: {str(e)}")
            conn.rollback()
            conn.close()
    
    def fetch_markets(self):
        """Fetch all available markets"""
        logger.info("Fetching all markets...")
        
        url = 'https://api.ox.fun/v3/markets'
        response = self.make_request(url)
        
        if not response or 'success' not in response or not response['success'] or 'data' not in response:
            logger.error("Failed to fetch markets. Using retry mechanism with longer timeout.")
            # Significantly longer timeout for markets list
            time.sleep(30)
            response = self.make_request(url)
            
            if not response or 'success' not in response or not response['success'] or 'data' not in response:
                logger.critical("Failed to fetch markets after multiple attempts. Exiting.")
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
            error_msg = "API error" if response else "No response"
            logger.error(f"Failed to fetch funding rate for {market_code}: {error_msg}")
            self.log_failed_request(market_code, error_msg)
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
    
    def retry_failed_requests(self):
        """Retry previously failed requests"""
        logger.info("Retrying previously failed requests...")
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Get all failed requests
            cursor.execute('''
            SELECT market_code, request_time FROM failed_requests
            ORDER BY request_time DESC
            ''')
            
            failed_requests = cursor.fetchall()
            logger.info(f"Found {len(failed_requests)} failed requests to retry")
            
            success_count = 0
            
            for market_code, _ in failed_requests:
                logger.info(f"Retrying request for {market_code}")
                
                # Add additional delay between retries
                time.sleep(2 + random.uniform(0, 1))
                
                if self.fetch_and_store_funding_rate(market_code):
                    success_count += 1
                    
                    # Remove from failed_requests if successful
                    cursor.execute('''
                    DELETE FROM failed_requests
                    WHERE market_code = ?
                    ''', (market_code,))
                    conn.commit()
            
            logger.info(f"Retry complete. Successfully processed {success_count}/{len(failed_requests)} failed requests")
            
        except Exception as e:
            logger.error(f"Error retrying failed requests: {str(e)}")
        finally:
            conn.close()
    
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
            
            # Export failed requests for reference
            query = """
                SELECT market_code, request_time, error_message
                FROM failed_requests
                ORDER BY request_time DESC
            """
            
            failed_df = pd.read_sql_query(query, conn)
            
            if not failed_df.empty:
                failed_file = os.path.join(dated_dir, f"ox_failed_requests_{time_str}.csv")
                failed_df.to_csv(failed_file, index=False)
                logger.info(f"Exported {len(failed_df)} failed requests to {failed_file}")
            
            conn.close()
            
        except Exception as e:
            logger.error(f"Error exporting data to CSV: {str(e)}")
    
    def check_collection_progress(self, futures_markets, success_count):
        """Check collection progress and decide if we should continue"""
        # If more than 50% of markets failed, consider extending retry attempts
        if success_count < len(futures_markets) * 0.5:
            logger.warning(f"Low success rate: {success_count}/{len(futures_markets)} ({success_count*100/len(futures_markets):.1f}%)")
            logger.info("Taking a longer pause before continuing with additional retries")
            # Take a longer pause (2-3 minutes) before continuing
            pause_time = random.uniform(120, 180)
            logger.info(f"Pausing for {pause_time:.1f} seconds")
            time.sleep(pause_time)
            return True
        return False
    
    def collect_funding_rates(self):
        """Collect funding rates for all futures markets"""
        collection_time = datetime.now()
        logger.info(f"Starting funding rate collection at {collection_time}")
        logger.info(f"Using base delay of {self.base_delay}s with backoff factor {self.backoff_factor} and max retries {self.max_retries}")
        
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
        batch_size = 20  # Process in smaller batches with pauses
        total_batches = (len(futures_markets) + batch_size - 1) // batch_size
        
        for batch_idx in range(total_batches):
            start_idx = batch_idx * batch_size
            end_idx = min((batch_idx + 1) * batch_size, len(futures_markets))
            batch = futures_markets[start_idx:end_idx]
            
            logger.info(f"Processing batch {batch_idx+1}/{total_batches} ({start_idx+1}-{end_idx}/{len(futures_markets)})")
            
            batch_success = 0
            for i, market_code in enumerate(batch):
                logger.info(f"Processing {start_idx+i+1}/{len(futures_markets)}: {market_code}")
                
                try:
                    # Add small random delay between requests in the same batch
                    if i > 0:
                        time.sleep(random.uniform(1.0, 2.0))
                        
                    if self.fetch_and_store_funding_rate(market_code):
                        success_count += 1
                        batch_success += 1
                    
                except Exception as e:
                    logger.error(f"Error processing {market_code}: {str(e)}")
                    self.log_failed_request(market_code, str(e))
            
            logger.info(f"Batch {batch_idx+1} complete: {batch_success}/{len(batch)} successful")
            
            # Add a pause between batches if we have more to process
            if batch_idx < total_batches - 1:
                # Longer pause if rate limited a lot in this batch
                if self.consecutive_rate_limits > 0:
                    pause_time = min(30 * self.consecutive_rate_limits, 180)
                    logger.info(f"Rate limited in this batch. Pausing for {pause_time} seconds before next batch")
                else:
                    pause_time = random.uniform(5, 15)
                    logger.info(f"Pausing for {pause_time:.1f} seconds before next batch")
                    
                time.sleep(pause_time)
        
        logger.info(f"Initial collection complete. Collected {success_count}/{len(futures_markets)} funding rates.")
        
        # Check if we need to retry with a different approach
        if self.check_collection_progress(futures_markets, success_count):
            logger.info("Attempting to retry with longer delays...")
            # Increase base delay for the retry phase
            self.base_delay *= 2
            # Retry previously failed requests
            self.retry_failed_requests()
        
        # Final retry for any remaining failed requests
        self.retry_failed_requests()
        
        # Export to CSV
        self.export_to_csv(collection_time)
        
        # Get final count of collected rates
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM funding_rates WHERE date(collection_time) = date('{collection_time.strftime('%Y-%m-%d')}')")
        final_count = cursor.fetchone()[0]
        conn.close()
        
        logger.info(f"Final collection stats: {final_count}/{len(futures_markets)} funding rates collected")
        
        return {
            'collection_time': collection_time,
            'markets_processed': len(futures_markets),
            'funding_rates_collected': final_count,
            'success_rate': (final_count / len(futures_markets)) * 100 if len(futures_markets) > 0 else 0,
            'rate_limit_hits': self.rate_limit_hits
        }

# Main execution
if __name__ == "__main__":
    logger.info("Starting OX Funding Rate Collector")
    
    # Parse command line arguments
    import argparse
    parser = argparse.ArgumentParser(description='OX Funding Rate Collector')
    parser.add_argument('--max-retries', type=int, default=5, help='Maximum number of retries per request')
    parser.add_argument('--base-delay', type=float, default=2.0, help='Base delay between requests in seconds')
    parser.add_argument('--backoff-factor', type=float, default=2.0, help='Exponential backoff factor')
    args = parser.parse_args()
    
    # Check for API credentials
    if API_KEY == "your_api_key" or SECRET_KEY == "your_secret_key":
        logger.error("API credentials not configured. Please set OX_API_KEY and OX_SECRET_KEY environment variables.")
        sys.exit(1)
    
    try:
        collector = OxFundingCollector()
        
        # Apply command line parameters if provided
        if args.max_retries:
            collector.max_retries = args.max_retries
        if args.base_delay:
            collector.base_delay = args.base_delay
        if args.backoff_factor:
            collector.backoff_factor = args.backoff_factor
        
        results = collector.collect_funding_rates()
        
        logger.info("Collection Summary:")
        logger.info(f"Collection time: {results['collection_time']}")
        logger.info(f"Markets processed: {results['markets_processed']}")
        logger.info(f"Funding rates collected: {results['funding_rates_collected']}")
        logger.info(f"Success rate: {results['success_rate']:.2f}%")
        logger.info(f"Rate limit hits: {results['rate_limit_hits']}")
        
        logger.info("OX Funding Rate Collector completed successfully")
        
        # Exit with failure status if success rate is too low
        if results['success_rate'] < 50:
            logger.warning("Success rate below 50%. Consider the collection incomplete.")
            sys.exit(2)
            
    except KeyboardInterrupt:
        logger.info("Collection interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.critical(f"Unexpected error: {str(e)}")
        sys.exit(1)
