import subprocess
import sys
import logging
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor
import pandas as pd
import json
import os
import ast
import re

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/app/logs/data_cleansing.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Function to install sqlalchemy
def install_sqlalchemy():
    package = "sqlalchemy==2.0.29"
    try:
        logger.info(f"Attempting to install {package}")
        subprocess.check_call([sys.executable, "-m", "pip", "install", package])
        logger.info(f"Successfully installed {package}")
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to install {package}: {str(e)}")
        raise

# Install sqlalchemy before importing
try:
    install_sqlalchemy()
except Exception as e:
    logger.error(f"Dependency installation failed: {str(e)}")
    sys.exit(1)

# Now import sqlalchemy after installation
from sqlalchemy import create_engine

class DataCleaner:
    def __init__(self, db_config, execution_date_nodash):
        """
        Initialize DataCleaner with database configuration
        
        Args:
            db_config (dict): Database configuration parameters
        """
        self.db_config = db_config
        self.engine = None
        self.connection = None
        # self.test_datetime = datetime.now().strftime("%Y%m%d%H%M%S")
        self.test_datetime = execution_date_nodash
        
    def connect_db(self):
        """Establish database connection"""
        try:
            # Create SQLAlchemy engine
            db_url = f"postgresql://{self.db_config['user']}:{self.db_config['password']}@{self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']}"
            self.engine = create_engine(db_url)
            
            # Create direct connection for table creation
            self.connection = psycopg2.connect(**self.db_config)
            # Set client encoding to UTF8
            self.connection.set_client_encoding('UTF8')
            logger.info("Database connection established successfully")
            
        except Exception as e:
            logger.error(f"Database connection failed: {str(e)}")
            raise
    
    def create_tables(self):
        """Create necessary tables in the database"""
        try:
            cursor = self.connection.cursor()
            
            # Create tables with proper data types
            create_table_sql = """
            -- Create data table for clean records
            CREATE TABLE IF NOT EXISTS data (
                dates DATE,
                ids VARCHAR(255) PRIMARY KEY,
                names VARCHAR(255),
                monthly_listeners BIGINT,
                popularity INTEGER,
                followers BIGINT,
                genres TEXT,
                first_release VARCHAR(4),
                last_release VARCHAR(4),
                num_releases INTEGER,
                num_tracks INTEGER,
                playlists_found VARCHAR(255),
                feat_track_ids TEXT
            );
            
            -- Create data_reject table for duplicate records
            CREATE TABLE IF NOT EXISTS data_reject (
                dates DATE,
                ids VARCHAR(255),
                names VARCHAR(255),
                monthly_listeners BIGINT,
                popularity INTEGER,
                followers BIGINT,
                genres TEXT,
                first_release VARCHAR(4),
                last_release VARCHAR(4),
                num_releases INTEGER,
                num_tracks INTEGER,
                playlists_found VARCHAR(255),
                feat_track_ids TEXT
            );
            """
            
            cursor.execute(create_table_sql)
            self.connection.commit()
            logger.info("Tables created successfully")
            
        except Exception as e:
            logger.error(f"Error creating tables: {str(e)}")
            raise
        finally:
            cursor.close()
    
    def read_csv(self, file_path):
        """Read CSV file and return DataFrame"""
        try:
            # Try reading CSV with UTF-8 encoding
            df = pd.read_csv(file_path, encoding='utf-8')
            logger.info(f"Successfully read CSV file: {file_path}")
            logger.info(f"Total rows in CSV: {len(df)}")
            return df
        except UnicodeDecodeError as ude:
            logger.warning(f"UTF-8 decoding failed: {str(ude)}. Falling back to reading with encoding error replacement.")
            # Fallback: Read file manually with error replacement
            with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
                df = pd.read_csv(f)
            logger.info(f"Successfully read CSV file with fallback: {file_path}")
            logger.info(f"Total rows in CSV: {len(df)}")
            return df
        except Exception as e:
            logger.error(f"Error reading CSV file: {str(e)}")
            raise
    
    def parse_genres(self, x):
        if pd.notna(x) and isinstance(x, str) and x.strip():
            if x.startswith('[') and x.endswith(']'):
                try:
                    val = ast.literal_eval(x)
                    if isinstance(val, list):
                        return [str(item).strip().encode('ascii', errors='ignore').decode('ascii') for item in val]
                except (SyntaxError, ValueError) as e:
                    logger.warning(f"parse_genres(): Failed to eval '{x}' — falling back to split — error: {e}")
            return [item.strip().encode('ascii', errors='ignore').decode('ascii') for item in x.split(',') if item.strip()]
        return []
    
    def parse_track_ids(self, x):
        if pd.notna(x) and isinstance(x, str) and x.strip():
            id_pattern = re.compile(r'^[a-zA-Z0-9]{22}$')
            items = [item.strip() for item in x.split(',')] if ',' in x else [x.strip()]
            valid_items = []
            invalid_items = []
            
            for item in items:
                if item and id_pattern.match(item):
                    valid_items.append(item.encode('ascii', errors='ignore').decode('ascii'))
                else:
                    invalid_items.append(item)
            
            if invalid_items:
                logger.warning(f"Invalid track IDs found: {invalid_items[:5]} {'and more' if len(invalid_items) > 5 else ''}")
            
            return valid_items
        return []
    
    def clean_data(self, df):
        try:
            df['dates'] = pd.to_datetime(df['dates'], dayfirst=True, errors='coerce').dt.strftime('%Y-%m-%d')
            invalid_dates = df[df['dates'].isna()]
            if not invalid_dates.empty:
                logger.warning(f"Found {len(invalid_dates)} rows with invalid dates: {invalid_dates[['ids', 'dates']].to_dict()}")

            df['names'] = df['names'].str.upper().str.encode('ascii', errors='ignore').str.decode('ascii')
            df['genres'] = df['genres'].apply(self.parse_genres)
            df['feat_track_ids'] = df['feat_track_ids'].apply(self.parse_track_ids)
            
            if df['playlists_found'].dtype in ['object', 'string']:
                df['playlists_found'] = df['playlists_found'].str.encode('ascii', errors='ignore').str.decode('ascii')
            else:
                df['playlists_found'] = df['playlists_found'].astype(str)
            
            duplicate_mask = df.duplicated(subset=['ids'], keep='first')
            clean_data = df[~duplicate_mask].copy()
            duplicate_data = df[duplicate_mask].copy()
            
            logger.info(f"Clean records: {len(clean_data)}")
            logger.info(f"Duplicate records: {len(duplicate_data)}")
            
            return clean_data, duplicate_data
            
        except Exception as e:
            logger.error(f"Error cleaning data: {str(e)}")
            raise
    
    def insert_to_database(self, clean_data, duplicate_data):
        """Insert data to database tables"""
        try:
            # Prepare data for database insertion
            clean_data_db = clean_data.copy()
            duplicate_data_db = duplicate_data.copy()
            
            # Convert lists back to strings for database storage
            clean_data_db['genres'] = clean_data_db['genres'].apply(lambda x: str(x))
            clean_data_db['feat_track_ids'] = clean_data_db['feat_track_ids'].apply(lambda x: str(x))
            duplicate_data_db['genres'] = duplicate_data_db['genres'].apply(lambda x: str(x))
            duplicate_data_db['feat_track_ids'] = duplicate_data_db['feat_track_ids'].apply(lambda x: str(x))
            
            # Insert clean data
            clean_data_db.to_sql('data', self.engine, if_exists='append', index=False)
            logger.info(f"Inserted {len(clean_data_db)} clean records to 'data' table")
            
            # Insert duplicate data
            duplicate_data_db.to_sql('data_reject', self.engine, if_exists='append', index=False)
            logger.info(f"Inserted {len(duplicate_data_db)} duplicate records to 'data_reject' table")
            
        except Exception as e:
            logger.error(f"Error inserting data to database: {str(e)}")
            raise
    
    def create_backup_files(self, clean_data, duplicate_data):
        """Create backup CSV and JSON files"""
        try:
            # Ensure target directory exists
            target_dir = os.path.join('/target')  # Platform-independent path
            os.makedirs(target_dir, exist_ok=True)
            
            # Create duplicate CSV file
            duplicate_csv_path = os.path.join('/target', f"data_reject_{self.test_datetime}.csv")
            duplicate_data_csv = duplicate_data.copy()
            
            # Convert lists back to string format for CSV
            duplicate_data_csv['genres'] = duplicate_data_csv['genres'].apply(lambda x: str(x))
            duplicate_data_csv['feat_track_ids'] = duplicate_data_csv['feat_track_ids'].apply(lambda x: str(x))
            
            duplicate_data_csv.to_csv(duplicate_csv_path, index=False)
            logger.info(f"Created duplicate CSV file: {duplicate_csv_path}")
            
            # Create clean JSON file
            clean_json_path = os.path.join('/target', f"data_{self.test_datetime}.json")
            json_data = {
                "row_count": len(clean_data),
                "data": []
            }
            
            for _, row in clean_data.iterrows():
                json_data["data"].append({
                    "dates": row['dates'],
                    "ids": str(row['ids']),
                    "names": str(row['names']),
                    "monthly_listeners": int(row['monthly_listeners']) if pd.notna(row['monthly_listeners']) else 0,
                    "popularity": int(row['popularity']) if pd.notna(row['popularity']) else 0,
                    "followers": int(row['followers']) if pd.notna(row['followers']) else 0,
                    "genres": row['genres'],
                    "first_release": str(row['first_release']),
                    "last_release": str(row['last_release']),
                    "num_releases": int(row['num_releases']) if pd.notna(row['num_releases']) else 0,
                    "num_tracks": int(row['num_tracks']) if pd.notna(row['num_tracks']) else 0,
                    "playlists_found": str(row['playlists_found']),
                    "feat_track_ids": row['feat_track_ids']
                })
            
            with open(clean_json_path, 'w') as f:
                json.dump(json_data, f, indent=2)
            
            logger.info(f"Created clean JSON file: {clean_json_path}")
            
        except Exception as e:
            logger.error(f"Error creating backup files: {str(e)}")
            raise
    
    def get_table_counts(self):
        """Get row counts from database tables"""
        try:
            cursor = self.connection.cursor()
            
            # Get count from data table
            cursor.execute("SELECT COUNT(*) FROM data")
            clean_count = cursor.fetchone()[0]
            
            # Get count from data_reject table
            cursor.execute("SELECT COUNT(*) FROM data_reject")
            duplicate_count = cursor.fetchone()[0]
            
            logger.info(f"Total rows in 'data' table: {clean_count}")
            logger.info(f"Total rows in 'data_reject' table: {duplicate_count}")
            
            cursor.close()
            return clean_count, duplicate_count
            
        except Exception as e:
            logger.error(f"Error getting table counts: {str(e)}")
            raise
    
    def close_connections(self):
        """Close database connections"""
        try:
            if self.connection:
                self.connection.close()
            if self.engine:
                self.engine.dispose()
            logger.info("Database connections closed")
        except Exception as e:
            logger.error(f"Error closing connections: {str(e)}")
    
    def run_pipeline(self, csv_path):
        """Run the complete data cleansing pipeline"""
        try:
            logger.info("Starting data cleansing pipeline...")
            
            # Connect to database
            self.connect_db()
            
            # Create tables
            self.create_tables()
            
            # Read CSV data
            df = self.read_csv(csv_path)
            
            # Clean data
            clean_data, duplicate_data = self.clean_data(df)
            
            # Insert to database
            self.insert_to_database(clean_data, duplicate_data)
            
            # Create backup files
            self.create_backup_files(clean_data, duplicate_data)
            
            # Get table counts
            self.get_table_counts()
            
            logger.info("Data cleansing pipeline completed successfully!")
            
        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}")
            raise
        finally:
            self.close_connections()

def main():
    """Main function to run the data cleansing pipeline"""
    
    # Database configuration
    db_config = {
        'host': 'postgres',
        'port': 5432,
        'database': 'data_cleansing',
        'user': 'postgres',
        'password': 'password'
    }
    
    # CSV file path
    csv_path = '/source/scrap.csv'
    
    execution_date_nodash = os.environ.get('EXECUTION_DATE_NODASH', datetime.now().strftime("%Y%m%d%H%M%S"))
    
    # Create DataCleaner instance and run pipeline
    cleaner = DataCleaner(db_config, execution_date_nodash)
    cleaner.run_pipeline(csv_path)

if __name__ == "__main__":
    main()