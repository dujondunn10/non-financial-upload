"""
Document Migration Script
Downloads files from URLs, renames them according to naming convention, and uploads to S3
"""

import os
import sys
import pandas as pd
import requests
import boto3
from datetime import datetime
from pathlib import Path
import tempfile
import shutil
from urllib.parse import urlparse, unquote
from dotenv import load_dotenv
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import random
from fake_useragent import UserAgent

# ==============================================================================
# CONFIGURATION - EDIT THIS SECTION
# ==============================================================================

# Specify your CSV file name here (file should be in the same folder as this script)
#CSV_FILENAME = "Exceptions and New Migration - Sheet1 (1).csv" # exceptions 
CSV_FILENAME = "Copy of For Migration (YES) - Sheet1.csv" 

# S3 bucket name
BUCKET_NAME = 'jse-renamed-docs-copy'

# Performance settings
MAX_WORKERS = 3  # Number of concurrent downloads (reduced to avoid detection)
RETRY_ATTEMPTS = 3  # Number of retry attempts for failed downloads
REQUEST_DELAY_MIN = 1  # Minimum delay between requests (seconds)
REQUEST_DELAY_MAX = 3  # Maximum delay between requests (seconds)

# ==============================================================================
# END CONFIGURATION
# ==============================================================================

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('migration.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Constants from configuration
RETRY_DELAY = 2  # seconds

# Initialize user agent generator
try:
    ua = UserAgent()
except:
    ua = None
    logger.warning("Could not initialize fake-useragent, using fallback user agents")

class DocumentMigrator:
    def __init__(self, csv_path):
        self.csv_path = csv_path
        self.s3_client = boto3.client('s3')
        self.temp_dir = None
        self.failed_downloads = []
        self.failed_uploads = []
        self.session = self._create_session()
        self.last_request_time = 0
        
    def _create_session(self):
        """Create a requests session with browser-like configuration"""
        session = requests.Session()
        
        # Set up retry adapter
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry
        
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        return session
    
    def _get_browser_headers(self):
        """Generate browser-like headers"""
        # Fallback user agents if fake-useragent fails
        fallback_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0'
        ]
        
        if ua:
            try:
                user_agent = ua.random
            except:
                user_agent = random.choice(fallback_agents)
        else:
            user_agent = random.choice(fallback_agents)
        
        headers = {
            'User-Agent': user_agent,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'max-age=0',
            'Pragma': 'no-cache'
        }
        
        # Randomly include or exclude certain headers to vary fingerprint
        if random.random() > 0.5:
            headers['Referer'] = 'https://www.google.com/'
        
        if random.random() > 0.3:
            headers['Sec-Ch-Ua'] = '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"'
            headers['Sec-Ch-Ua-Mobile'] = '?0'
            headers['Sec-Ch-Ua-Platform'] = '"Windows"' if 'Windows' in user_agent else '"macOS"'
        
        return headers
    
    def _add_request_delay(self):
        """Add random delay between requests to mimic human behavior"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        if time_since_last < REQUEST_DELAY_MIN:
            delay = random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX)
            time.sleep(delay)
        
        self.last_request_time = time.time()
        
    def create_temp_directory(self):
        """Create a temporary directory for downloads"""
        self.temp_dir = tempfile.mkdtemp(prefix='doc_migration_')
        logger.info(f"Created temporary directory: {self.temp_dir}")
        return self.temp_dir
    
    def cleanup_temp_directory(self):
        """Remove temporary directory"""
        if self.temp_dir and os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
            logger.info(f"Cleaned up temporary directory: {self.temp_dir}")
    
    def download_file(self, url, temp_path, row_index):
        """Download a file with retry logic and anti-bot measures"""
        for attempt in range(RETRY_ATTEMPTS):
            try:
                # Add delay between requests
                self._add_request_delay()
                
                # Get fresh headers for each request
                headers = self._get_browser_headers()
                
                # Parse URL to get domain for referer
                parsed_url = urlparse(url)
                if parsed_url.netloc:
                    headers['Host'] = parsed_url.netloc
                    if random.random() > 0.3:
                        headers['Referer'] = f"{parsed_url.scheme}://{parsed_url.netloc}/"
                
                # Make request with timeout and streaming
                response = self.session.get(
                    url, 
                    headers=headers,
                    timeout=(10, 30),  # (connection timeout, read timeout)
                    stream=True,
                    allow_redirects=True,
                    verify=True
                )
                response.raise_for_status()
                
                # Check content type to ensure we're downloading a file
                content_type = response.headers.get('Content-Type', '')
                if 'text/html' in content_type.lower():
                    logger.warning(f"Received HTML instead of file for {url}, might be blocked")
                    # Try with different headers on next attempt
                    if attempt < RETRY_ATTEMPTS - 1:
                        time.sleep(random.uniform(3, 5))
                        continue
                
                # Download with progress tracking for large files
                total_size = int(response.headers.get('Content-Length', 0))
                downloaded = 0
                
                with open(temp_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            downloaded += len(chunk)
                            
                            # Log progress for large files
                            if total_size > 0 and downloaded % (1024 * 1024) == 0:
                                progress = (downloaded / total_size) * 100
                                logger.debug(f"Download progress for {url}: {progress:.1f}%")
                
                # Verify file was actually downloaded
                if os.path.getsize(temp_path) == 0:
                    raise Exception("Downloaded file is empty")
                
                logger.info(f"Downloaded: {url} -> {temp_path} ({os.path.getsize(temp_path)} bytes)")
                return True
                
            except requests.exceptions.TooManyRedirects:
                logger.error(f"Too many redirects for {url}")
                self.failed_downloads.append({'row': row_index, 'url': url, 'error': 'Too many redirects'})
                return False
                
            except requests.exceptions.Timeout:
                logger.warning(f"Timeout on attempt {attempt + 1} for {url}")
                if attempt < RETRY_ATTEMPTS - 1:
                    time.sleep(random.uniform(5, 10))
                else:
                    self.failed_downloads.append({'row': row_index, 'url': url, 'error': 'Timeout'})
                    return False
                    
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1} failed for {url}: {str(e)}")
                if attempt < RETRY_ATTEMPTS - 1:
                    # Exponential backoff with jitter
                    delay = RETRY_DELAY * (2 ** attempt) + random.uniform(0, 2)
                    time.sleep(delay)
                else:
                    logger.error(f"Failed to download after {RETRY_ATTEMPTS} attempts: {url}")
                    self.failed_downloads.append({'row': row_index, 'url': url, 'error': str(e)})
                    return False
    
    def format_date(self, date_string):
        """Convert date string to YYYY-MM-DD format"""
        try:
            # Try parsing different date formats
            date_formats = [
                '%Y-%m-%d %H:%M:%S',
                '%Y/%m/%d %H:%M:%S', 
                '%Y-%m-%d',
                '%d/%m/%Y',
                '%d/%m/%Y %H:%M',  # Added for "24/6/2022 14:09" format
                '%m/%d/%Y',
                '%Y/%m/%d'
            ]
            
            for fmt in date_formats:
                try:
                    # Remove any fractional seconds
                    clean_date = str(date_string).split('.')[0]
                    dt = datetime.strptime(clean_date, fmt)
                    return dt.strftime('%Y-%m-%d')
                except ValueError:
                    continue
            
            # If all formats fail, return a cleaned version
            logger.warning(f"Could not parse date: {date_string}, using fallback")
            # Remove time portion if present and replace slashes with dashes
            date_part = str(date_string).split()[0] if ' ' in str(date_string) else str(date_string)
            # Replace forward slashes with dashes for safe filename
            return date_part.replace('/', '-')
            
        except Exception as e:
            logger.error(f"Error formatting date {date_string}: {str(e)}")
            # Return a safe fallback
            return "unknown-date"
    
    def generate_filename(self, row):
        """Generate filename according to naming convention from CSV"""
        try:
            # Get the naming convention from the CSV
            naming_convention = str(row['Naming Convention'])
            
            # Extract components
            company_name = str(row['InstrumentName']).lower().replace(' ', '_').replace('/', '_')
            symbol = str(row['InstrumentCode']).lower()
            financial_year = str(row['Year'])
            posting_date = self.format_date(row['post_date'])
            
            # Log the naming convention being used
            logger.debug(f"Using naming convention: '{naming_convention}'")
            
            # Replace placeholders in naming convention (case-sensitive)
            filename = naming_convention
            
            # Replace each placeholder - maintain the exact format from the CSV
            filename = filename.replace('company_name', company_name)
            filename = filename.replace('symbol', symbol)
            filename = filename.replace('Financial Year', financial_year)
            filename = filename.replace('Posting Date', posting_date)
            
            # Handle spaces around dashes (e.g., " - " becomes "_")
            filename = filename.replace(' - ', '_')
            filename = filename.replace(' ', '_')
            
            # Get original extension from URL if not in naming convention
            if not filename.endswith(('.pdf', '.PDF', '.doc', '.docx', '.xls', '.xlsx')):
                url = row['guid']
                parsed_url = urlparse(url)
                path = unquote(parsed_url.path)
                _, ext = os.path.splitext(path)
                if not ext:
                    ext = '.pdf'
                filename += ext
            
            # Clean filename of any invalid characters for Windows/Linux compatibility
            invalid_chars = '<>:"|?*\\/\t\n\r'
            for char in invalid_chars:
                filename = filename.replace(char, '_')
            
            # Replace multiple underscores with single underscore
            while '__' in filename:
                filename = filename.replace('__', '_')
            
            # Ensure filename doesn't exceed OS limits (255 chars typically)
            if len(filename) > 200:
                # Truncate company name if filename is too long
                name_part = filename[:150]
                ext_part = filename[-50:]
                filename = name_part + "..." + ext_part
            
            logger.info(f"Generated filename: {filename}")
            
            return filename
            
        except Exception as e:
            logger.error(f"Error generating filename for row: {str(e)}")
            logger.error(f"Row data: InstrumentName={row.get('InstrumentName')}, InstrumentCode={row.get('InstrumentCode')}, Year={row.get('Year')}, Naming Convention={row.get('Naming Convention')}")
            # Fallback to default naming if there's an error
            try:
                company_name = str(row['InstrumentName']).lower().replace(' ', '_').replace('/', '_')
                symbol = str(row['InstrumentCode']).lower()
                financial_year = str(row['Year'])
                posting_date = self.format_date(row['post_date'])
                
                url = row['guid']
                parsed_url = urlparse(url)
                path = unquote(parsed_url.path)
                _, ext = os.path.splitext(path)
                if not ext:
                    ext = '.pdf'
                
                filename = f"{company_name}_{symbol}_{financial_year}_{posting_date}{ext}"
                
                # Clean filename
                invalid_chars = '<>:"|?*\\/\t\n\r'
                for char in invalid_chars:
                    filename = filename.replace(char, '_')
                
                logger.warning(f"Using fallback naming convention: {filename}")
                return filename
            except:
                raise
    
    def generate_s3_path(self, row, filename):
        """Generate S3 path for the file"""
        try:
            instrument_code = str(row['InstrumentCode'])
            folder_name = str(row['Folder_Name'])
            year = str(row['Year'])
            
            s3_path = f"organized/{instrument_code}/{folder_name}/{year}/{filename}"
            return s3_path
            
        except Exception as e:
            logger.error(f"Error generating S3 path for row: {str(e)}")
            raise
    
    def upload_to_s3(self, local_path, s3_path, row_index):
        """Upload file to S3 with retry logic"""
        for attempt in range(RETRY_ATTEMPTS):
            try:
                self.s3_client.upload_file(local_path, BUCKET_NAME, s3_path)
                logger.info(f"Uploaded to S3: s3://{BUCKET_NAME}/{s3_path}")
                return True
                
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1} failed for S3 upload: {str(e)}")
                if attempt < RETRY_ATTEMPTS - 1:
                    time.sleep(RETRY_DELAY)
                else:
                    logger.error(f"Failed to upload after {RETRY_ATTEMPTS} attempts: {s3_path}")
                    self.failed_uploads.append({'row': row_index, 's3_path': s3_path, 'error': str(e)})
                    return False
    
    def process_row(self, row, row_index):
        """Process a single row: download, rename, and upload"""
        try:
            # Skip if no URL
            if pd.isna(row['guid']) or not str(row['guid']).strip():
                logger.warning(f"Row {row_index}: No URL found, skipping")
                return False
            
            url = str(row['guid']).strip()
            
            # Generate filename
            filename = self.generate_filename(row)
            temp_path = os.path.join(self.temp_dir, f"{row_index}_{filename}")
            
            # Download file
            if not self.download_file(url, temp_path, row_index):
                return False
            
            # Generate S3 path
            s3_path = self.generate_s3_path(row, filename)
            
            # Upload to S3
            if not self.upload_to_s3(temp_path, s3_path, row_index):
                return False
            
            # Clean up temp file
            try:
                os.remove(temp_path)
            except:
                pass
            
            return True
            
        except Exception as e:
            logger.error(f"Error processing row {row_index}: {str(e)}")
            return False
    
    def run(self):
        """Main execution method"""
        try:
            # Read CSV
            logger.info(f"Reading CSV file: {self.csv_path}")
            df = pd.read_csv(self.csv_path)
            logger.info(f"Found {len(df)} rows to process")
            
            # Create temp directory
            self.create_temp_directory()
            
            # Process rows in parallel
            successful = 0
            failed = 0
            
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                # Submit all tasks
                future_to_row = {
                    executor.submit(self.process_row, row, idx): (idx, row) 
                    for idx, row in df.iterrows()
                }
                
                # Process completed tasks
                for future in as_completed(future_to_row):
                    idx, row = future_to_row[future]
                    try:
                        if future.result():
                            successful += 1
                        else:
                            failed += 1
                    except Exception as e:
                        logger.error(f"Exception for row {idx}: {str(e)}")
                        failed += 1
                    
                    # Progress update
                    if (successful + failed) % 10 == 0:
                        logger.info(f"Progress: {successful + failed}/{len(df)} processed")
            
            # Final summary
            logger.info("=" * 50)
            logger.info(f"Migration completed!")
            logger.info(f"Total rows: {len(df)}")
            logger.info(f"Successful: {successful}")
            logger.info(f"Failed: {failed}")
            
            # Report failures
            if self.failed_downloads:
                logger.error(f"Failed downloads: {len(self.failed_downloads)}")
                for fail in self.failed_downloads[:5]:  # Show first 5
                    logger.error(f"  Row {fail['row']}: {fail['url']} - {fail['error']}")
            
            if self.failed_uploads:
                logger.error(f"Failed uploads: {len(self.failed_uploads)}")
                for fail in self.failed_uploads[:5]:  # Show first 5
                    logger.error(f"  Row {fail['row']}: {fail['s3_path']} - {fail['error']}")
            
            # Save failure report
            if self.failed_downloads or self.failed_uploads:
                self.save_failure_report()
            
        except Exception as e:
            logger.error(f"Fatal error: {str(e)}")
            raise
        finally:
            self.cleanup_temp_directory()
    
    def save_failure_report(self):
        """Save detailed failure report to CSV"""
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            if self.failed_downloads:
                pd.DataFrame(self.failed_downloads).to_csv(
                    f'failed_downloads_{timestamp}.csv', index=False
                )
                logger.info(f"Saved download failures to: failed_downloads_{timestamp}.csv")
            
            if self.failed_uploads:
                pd.DataFrame(self.failed_uploads).to_csv(
                    f'failed_uploads_{timestamp}.csv', index=False
                )
                logger.info(f"Saved upload failures to: failed_uploads_{timestamp}.csv")
                
        except Exception as e:
            logger.error(f"Error saving failure report: {str(e)}")


def main():
    """Main entry point - uses the CSV file specified in the configuration"""
    # Build the full path to the CSV file
    script_dir = os.path.dirname(os.path.abspath(__file__))
    csv_filepath = os.path.join(script_dir, CSV_FILENAME)
    
    # Check if file exists
    if not os.path.exists(csv_filepath):
        logger.error(f"CSV file not found: {csv_filepath}")
        logger.error(f"Please make sure '{CSV_FILENAME}' is in the same folder as this script")
        logger.error(f"Script location: {script_dir}")
        sys.exit(1)
    
    logger.info(f"Starting migration with CSV file: {CSV_FILENAME}")
    logger.info(f"Full path: {csv_filepath}")
    
    # Check for AWS credentials
    if not os.getenv('AWS_ACCESS_KEY_ID') or not os.getenv('AWS_SECRET_ACCESS_KEY'):
        logger.warning("AWS credentials not found in environment. Attempting to use default credentials...")
    
    # Install fake-useragent if not available
    try:
        import fake_useragent
    except ImportError:
        logger.info("Installing fake-useragent for better anti-detection...")
        os.system("pip install fake-useragent")
    
    # Run migration
    try:
        migrator = DocumentMigrator(csv_filepath)
        migrator.run()
        logger.info("Migration completed successfully!")
    except Exception as e:
        logger.error(f"Migration failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()