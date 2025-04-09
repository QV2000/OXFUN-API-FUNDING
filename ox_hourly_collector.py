import requests
import time
import pandas as pd
from datetime import datetime
import os
import sqlite3
import logging
import random
import sys

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("ox_funding_collector.log")
    ]
)
logger = logging.getLogger("ox_funding_collector")

API_KEY = os.environ.get("OX_API_KEY", "your_api_key")
SECRET_KEY = os.environ.get("OX_SECRET_KEY", "your_secret_key")

DATA_DIR = os.environ.get("DATA_DIR", "data")
DB_PATH = os.path.join(DATA_DIR, "ox_funding_data.db")

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
        
        self.requests_per_minute = 20
        self.request_interval = 60.0 / self.requests_per_minute
        self.safety_factor = 1.2
        
        self.request_interval *= self.safety_factor
        
        self.max_retries = 5
        self.backoff_factor = 2.5
        self.jitter = 0.3
        
        self.rate_limit_hits = 0
        self.consecutive_rate_limits = 0
        self.max_rate_limit_pause = 300
        
        self.batch_size = 15
        self.batch_interval = 6.0
        self.last_request_time = 0
        self.request_timestamps = []
        
        self.init_database()
        
        logger.info(f"Rate limit configuration: {self.requests_per_minute} requests/min = {self.request_interval:.2f}s between requests")
    
    def init_database(self):
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
        base_retry_delay = self.request_interval * 2
        delay = base_retry_delay * (self.backoff_factor ** retry_count)
        jitter_amount = delay * self.jitter
        delay += random.uniform(-jitter_amount, jitter_amount)
        return max(base_retry_delay, delay)
    
    def adaptive_rate_limit_pause(self):
        self.consecutive_rate_limits += 1
        self.rate_limit_hits += 1
        
        if self.rate_limit_hits % 3 == 0:
            pause_time = 180 + random.uniform(30, 60)
            logger.warning(f"Rate limit threshold reached ({self.rate_limit_hits} total). Taking an extended break for {pause_time:.1f} seconds")
            time.sleep(pause_time)
            return True
        
        if self.consecutive_rate_limits > 1:
            pause_time = min(
                30 * (2 ** (self.consecutive_rate_limits - 1)), 
                self.max_rate_limit_pause
            )
            logger.warning(f"Multiple consecutive rate limits detected ({self.consecutive_rate_limits}). Pausing for {pause_time:.1f} seconds")
            time.sleep(pause_time)
            return True
            
        pause_time = 45 + random.uniform(5, 15)
        logger.warning(f"Rate limit detected. Pausing for {pause_time:.1f} seconds")
        time.sleep(pause_time)
        return True
    
    def enforce_rate_limit(self):
        current_time = time.time()
        
        one_minute_ago = current_time - 60
        self.request_timestamps = [t for t in self.request_timestamps if t > one_minute_ago]
        
        if len(self.request_timestamps) >= self.requests_per_minute:
            oldest_timestamp = min(self.request_timestamps)
            wait_time = oldest_timestamp + 60 - current_time + 0.1
            
            logger.warning(f"Rate limit window full ({len(self.request_timestamps)} requests in last minute). Waiting {wait_time:.2f}s")
            time.sleep(wait_time)
            
            current_time = time.time()
            one_minute_ago = current_time - 60
            self.request_timestamps = [t for t in self.request_timestamps if t > one_minute_ago]
        
        time_since_last_request = current_time - self.last_request_time
        if time_since_last_request < self.request_interval:
            wait_time = self.request_interval - time_since_last_request
            logger.debug(f"Waiting {wait_time:.2f}s to maintain request interval")
            time.sleep(wait_time)
        
        self.last_request_time = time.time()
        self.request_timestamps.append(self.last_request_time)
        
        return self.last_request_time
    
    def make_request(self, url, retry_count=0):
        if retry_count >= self.max_retries:
            logger.warning(f"Max retries ({self.max_retries}) exceeded for URL: {url}")
            return None
        
        try:
            self.enforce_rate_limit()
            
            if retry_count > 0:
                delay = self.calculate_backoff_delay(retry_count)
                logger.info(f"Retry {retry_count}/{self.max_retries}. Waiting {delay:.2f} seconds before retry.")
                time.sleep(delay)
                self.enforce_rate_limit()
            
            if retry_count == 0:
                logger.debug(f"Making request ({len(self.request_timestamps)}/{self.requests_per_minute} in current window)")
            
            response = requests.get(url, headers=self.headers, timeout=30)
            
            if response.status_code == 200:
                self.consecutive_rate_limits = 0
                return response.json()
            elif response.status_code == 429:
                self.rate_limit_hits += 1
                self.consecutive_rate_limits += 1
                
                logger.warning(f"Rate limited ({self.rate_limit_hits} total, {self.consecutive_rate_limits} consecutive)")
                
                retry_after = response.headers.get('Retry-After')
                if retry_after:
                    wait_time = int(retry_after) + 5
                    logger.info(f"Server requested wait time of {retry_after}s. Waiting {wait_time}s.")
                    time.sleep(wait_time)
                else:
                    wait_time = 60 * min(self.consecutive_rate_limits, 5)
                    logger.info(f"No Retry-After header. Waiting {wait_time}s for rate limit to reset.")
                    time.sleep(wait_time)
                
                self.request_timestamps = []
                
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
        logger.info("Fetching all markets...")
        
        url = 'https://api.ox.fun/v3/markets'
        response = self.make_request(url)
        
        if not response or 'success' not in response or not response['success'] or 'data' not in response:
            logger.error("Failed to fetch markets. Using retry mechanism with longer timeout.")
            time.sleep(30)
            response = self.make_request(url)
            
            if not response or 'success' not in response or not response['success'] or 'data' not in response:
                logger.critical("Failed to fetch markets after multiple attempts. Exiting.")
                return []
        
        markets_data = response['data']
        logger.info(f"Found {len(markets_data)} markets")
        
        futures_markets = [m['marketCode'] for m in markets_data if 'marketCode' in m and '-SWAP-LIN' in m['marketCode']]
        logger.info(f"Found {len(futures_markets)} futures markets")
        
        return futures_markets
    
    def fetch_and_store_funding_rate(self, market_code):
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
        
        return self.store_funding_rate(market_code, funding_rate, created_at)
    
    def store_funding_rate(self, market_code, funding_rate, created_at):
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
        logger.info("Retrying previously failed requests...")
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
            SELECT market_code, request_time FROM failed_requests
            ORDER BY request_time DESC
            ''')
            
            failed_requests = cursor.fetchall()
            logger.info(f"Found {len(failed_requests)} failed requests to retry")
            
            success_count = 0
            
            for market_code, _ in failed_requests:
                logger.info(f"Retrying request for {market_code}")
                
                if self.fetch_and_store_funding_rate(market_code):
                    success_count += 1
                    
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
        time_str = collection_time.strftime('%Y%m%d_%H%M%S')
        
        dated_dir = os.path.join(DATA_DIR, collection_time.strftime('%Y-%m-%d'))
        os.makedirs(dated_dir, exist_ok=True)
        
        try:
            conn = sqlite3.connect(self.db_path)
            
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
            
            consolidated_file = os.path.join(dated_dir, f"ox_funding_rates_{collection_time.strftime('%Y-%m-%d')}_consolidated.csv")
            
            if os.path.exists(consolidated_file):
                existing_df = pd.read_csv(consolidated_file)
                existing_df = existing_df[~existing_df['market_code'].isin(funding_df['market_code'])]
                consolidated_df = pd.concat([existing_df, funding_df])
            else:
                consolidated_df = funding_df
                
            consolidated_df.to_csv(consolidated_file, index=False)
            logger.info(f"Updated consolidated file with {len(funding_df)} new records")
            
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
    
    def collect_funding_rates(self):
        collection_time = datetime.now()
        logger.info(f"Starting funding rate collection at {collection_time}")
        logger.info(f"Rate limit: {self.requests_per_minute} requests/minute with {self.request_interval:.2f}s between requests")
        
        futures_markets = self.fetch_markets()
        
        if not futures_markets:
            logger.error("No futures markets found. Aborting collection.")
            return {
                'collection_time': collection_time,
                'markets_processed': 0,
                'funding_rates_collected': 0
            }
        
        estimated_time_seconds = len(futures_markets) * self.request_interval
        estimated_time_minutes = estimated_time_seconds / 60
        logger.info(f"Found {len(futures_markets)} markets. Estimated completion time: {estimated_time_minutes:.1f} minutes")
        
        success_count = 0
        total_batches = (len(futures_markets) + self.batch_size - 1) // self.batch_size
        
        logger.info(f"Processing in {total_batches} batches of {self.batch_size} markets each")
        
        for batch_idx in range(total_batches):
            start_idx = batch_idx * self.batch_size
            end_idx = min((batch_idx + 1) * self.batch_size, len(futures_markets))
            batch = futures_markets[start_idx:end_idx]
            
            logger.info(f"Processing batch {batch_idx+1}/{total_batches} ({start_idx+1}-{end_idx}/{len(futures_markets)})")
            
            batch_start_time = time.time()
            batch_success = 0
            
            for i, market_code in enumerate(batch):
                logger.info(f"Processing {start_idx+i+1}/{len(futures_markets)}: {market_code}")
                
                try:
                    if self.fetch_and_store_funding_rate(market_code):
                        success_count += 1
                        batch_success += 1
                
                except Exception as e:
                    logger.error(f"Error processing {market_code}: {str(e)}")
                    self.log_failed_request(market_code, str(e))
            
            batch_duration = time.time() - batch_start_time
            logger.info(f"Batch {batch_idx+1} complete: {batch_success}/{len(batch)} successful in {batch_duration:.1f}s")
            
            elapsed_time = time.time() - collection_time.timestamp()
            progress_percent = (batch_idx + 1) / total_batches * 100
            markets_per_second = (start_idx + len(batch)) / elapsed_time if elapsed_time > 0 else 0
            
            markets_remaining = len(futures_markets) - (start_idx + len(batch))
            estimated_remaining_seconds = markets_remaining / markets_per_second if markets_per_second > 0 else 0
            estimated_remaining_minutes = estimated_remaining_seconds / 60
            
            logger.info(f"Progress: {progress_percent:.1f}% complete. Estimated remaining time: {estimated_remaining_minutes:.1f} minutes")
            
            if batch_idx < total_batches - 1:
                pause_time = self.batch_interval + random.uniform(0, 2)
                logger.info(f"Pausing for {pause_time:.1f}s before next batch")
                time.sleep(pause_time)
                
                if self.rate_limit_hits > 0 and batch_idx % 5 == 4:
                    pause_time = 60 + random.uniform(0, 15)
                    logger.info(f"Taking an extended break of {pause_time:.1f}s to ensure rate limit recovery")
                    time.sleep(pause_time)
                    self.request_timestamps = []
        
        logger.info(f"Initial collection complete. Collected {success_count}/{len(futures_markets)} funding rates.")
        
        failed_count = len(futures_markets) - success_count
        if failed_count > 0:
            logger.info(f"Found {failed_count} failed requests. Waiting 2 minutes before retrying...")
            time.sleep(120)
            self.retry_failed_requests()
        
        self.export_to_csv(collection_time)
        
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

if __name__ == "__main__":
    logger.info("Starting OX Funding Rate Collector")
    
    import argparse
    parser = argparse.ArgumentParser(description='OX Funding Rate Collector')
    parser.add_argument('--max-retries', type=int, default=5, help='Maximum number of retries per request')
    parser.add_argument('--requests-per-minute', type=int, default=20, help='API rate limit (requests per minute)')
    parser.add_argument('--safety-factor', type=float, default=1.2, help='Safety factor for rate limit (>1 is more conservative)')
    parser.add_argument('--batch-size', type=int, default=15, help='Number of markets to process in each batch')
    parser.add_argument('--batch-interval', type=float, default=6.0, help='Additional seconds to wait between batches')
    parser.add_argument('--resume-failed', action='store_true', help='Only retry previously failed requests')
    args = parser.parse_args()
    
    if API_KEY == "your_api_key" or SECRET_KEY == "your_secret_key":
        logger.error("API credentials not configured. Please set OX_API_KEY and OX_SECRET_KEY environment variables.")
        sys.exit(1)
    
    try:
        collector = OxFundingCollector()
        
        if args.max_retries:
            collector.max_retries = args.max_retries
        if args.requests_per_minute:
            collector.requests_per_minute = args.requests_per_minute
            collector.request_interval = 60.0 / collector.requests_per_minute * collector.safety_factor
            logger.info(f"Set rate limit to {collector.requests_per_minute} requests/minute ({collector.request_interval:.2f}s between requests)")
        if args.safety_factor:
            collector.safety_factor = args.safety_factor
            collector.request_interval = 60.0 / collector.requests_per_minute * collector.safety_factor
            logger.info(f"Set safety factor to {collector.safety_factor} ({collector.request_interval:.2f}s between requests)")
        if args.batch_size:
            collector.batch_size = args.batch_size
        if args.batch_interval:
            collector.batch_interval = args.batch_interval
        
        if args.resume_failed:
            logger.info("Only retrying previously failed requests")
            collector.retry_failed_requests()
            sys.exit(0)
        
        results = collector.collect_funding_rates()
        
        logger.info("Collection Summary:")
        logger.info(f"Collection time: {results['collection_time']}")
        logger.info(f"Markets processed: {results['markets_processed']}")
        logger.info(f"Funding rates collected: {results['funding_rates_collected']}")
        logger.info(f"Success rate: {results['success_rate']:.2f}%")
        logger.info(f"Rate limit hits: {results['rate_limit_hits']}")
        
        logger.info("OX Funding Rate Collector completed successfully")
        
        if results['success_rate'] < 50:
            logger.warning("Success rate below 50%. Consider the collection incomplete.")
            sys.exit(2)
            
    except KeyboardInterrupt:
        logger.info("Collection interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.critical(f"Unexpected error: {str(e)}")
        sys.exit(1)
