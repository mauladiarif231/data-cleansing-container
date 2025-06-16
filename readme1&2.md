# CSV Data Cleansing Application

## Overview

This application is designed to clean CSV data by removing duplicate records based on unique IDs, transforming the data into proper formats, and storing both clean and duplicate records in separate database tables. The application also creates backup files in CSV and JSON formats.

## Features

- **Data Deduplication**: Removes duplicate records based on the `ids` column
- **Data Transformation**: Converts data types and formats according to specifications
- **Database Integration**: Stores clean and duplicate data in PostgreSQL tables
- **Backup Generation**: Creates CSV files for duplicates and JSON files for clean data
- **Error Handling**: Comprehensive error handling with logging
- **Containerization**: Fully containerized application with Docker
- **Testing**: Unit tests with pytest framework

## Project Structure

```
├── main.py                
├── ddl.sql                 
├── requirements.txt        
├── Dockerfile             
├── docker-compose.yaml    
├── test_main.py           
├── pytest.ini            
├── README.md              
├── source/                
│   └── scrap.csv          
└── target/                
    ├── data_YYYYMMDDHHMMSS.json
    └── data_reject_YYYYMMDDHHMMSS.csv
```

## Requirements

### System Requirements
- Docker and Docker Compose
- Python 3.11+ (if running locally)
- PostgreSQL (if running locally)

### Python Dependencies
- pandas
- psycopg2-binary==2.9.9
- python-dotenv==1.0.0
- pytest==7.4.3
- pytest-cov==4.1.0
- sqlalchemy==2.0.23

## Installation & Setup

### Method 1: Using Docker Compose (Recommended)

1. **Clone or prepare the project files**
   ```bash
   # Ensure all project files are in the same directory
   # Place your source CSV file in ./source/scrap.csv
   ```

2. **Build and run the application**
   ```bash
   # Build and start all services
   docker-compose up --build
   
   # Or run in detached mode
   docker-compose up --build -d
   ```

3. **Check the logs**
   ```bash
   docker-compose logs data-cleaner
   ```

4. **Stop the services**
   ```bash
   docker-compose down
   ```

### Method 2: Local Installation

1. **Install PostgreSQL**
   ```bash
   # Ubuntu/Debian
   sudo apt-get install postgresql postgresql-contrib
   
   # macOS
   brew install postgresql
   ```

2. **Create database**
   ```bash
   # Login to PostgreSQL
   sudo -u postgres psql
   
   # Create database and user
   CREATE DATABASE data_cleansing;
   CREATE USER postgres WITH PASSWORD 'password';
   GRANT ALL PRIVILEGES ON DATABASE data_cleansing TO postgres;
   \q
   ```

3. **Install Python dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Run the application**
   ```bash
   python main.py
   ```

## How to Run

### Using Docker Compose

```bash
# Start the complete environment
docker-compose up --build

# The application will:
# 1. Start PostgreSQL database
# 2. Create necessary tables
# 3. Process the CSV file
# 4. Generate output files
```

### Using Docker Run Commands

```bash
# Build the image
docker build -t data-cleaner .

# Run PostgreSQL
docker run -d \
  --name postgres-db \
  -e POSTGRES_DB=data_cleansing \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=password \
  -p 5432:5432 \
  postgres:15-alpine

# Run the application
docker run --rm \
  --link postgres-db:postgres \
  -v $(pwd)/source:/source \
  -v $(pwd)/target:/target \
  -e DB_HOST=postgres \
  data-cleaner
```

## Configuration

### Database Configuration

The application uses the following default database configuration:

```python
db_config = {
    'host': 'localhost',      # or 'postgres' in Docker ya
    'port': 5432,
    'database': 'data_cleansing',
    'user': 'postgres',
    'password': 'password'
}
```

### Environment Variables

You can override database settings using environment variables:

- `DB_HOST`: Database host (default: localhost)
- `DB_PORT`: Database port (default: 5432)
- `DB_NAME`: Database name (default: data_cleansing)
- `DB_USER`: Database user (default: postgres)
- `DB_PASSWORD`: Database password (default: password)

## Input/Output

### Input
- **File**: `/source/scrap.csv`
- **Format**: CSV with columns matching the expected schema
- **Required Columns**: dates, ids, names, monthly_listeners, popularity, followers, genres, first_release, last_release, num_releases, num_tracks, playlists_found, feat_track_ids

### Output

#### Database Tables
1. **`data`** - Clean records with unique IDs
2. **`data_reject`** - Duplicate records

#### Backup Files
1. **`/target/data_reject_YYYYMMDDHHMMSS.csv`** - Duplicate records in CSV format
2. **`/target/data_YYYYMMDDHHMMSS.json`** - Clean records in JSON format

### JSON Output Format
```json
{
  "row_count": 100,
  "data": [
    {
      "dates": "2024-01-01",
      "ids": "1",
      "names": "ARTIST NAME",
      "monthly_listeners": 1000000,
      "popularity": 80,
      "followers": 500000,
      "genres": ["pop", "rock"],
      "first_release": "2020",
      "last_release": "2024",
      "num_releases": 5,
      "num_tracks": 50,
      "playlists_found": "100",
      "feat_track_ids": ["track1", "track2"]
    }
  ]
}
```

## Data Processing Rules

1. **Deduplication**: Records with duplicate `ids` are identified and separated
2. **Data Transformation**:
   - `dates`: Converted to YYYY-MM-DD format
   - `names`: Converted to uppercase
   - `genres`: Parsed as list of strings
   - `feat_track_ids`: Parsed as list of strings
   - Numeric fields: Converted to appropriate integer/bigint types

3. **Database Storage**:
   - Clean records: Stored in `data` table with `ids` as primary key
   - Duplicate records: Stored in `data_reject` table with auto-increment ID

## Testing

### Running Unit Tests

```bash
# Install pytest
pip install pytest

# Run all tests
pytest

# Run tests with verbose output
pytest -v

# Run specific test file
pytest test_main.py

# Run tests with coverage
pip install pytest-cov
pytest --cov=main
```

### Test Coverage

The test suite covers:
- CSV reading functionality
- Data cleaning and deduplication logic
- Database connection handling
- File creation operations
- Error handling scenarios

## Database Schema

### Table: `data` (Clean Records)
```sql
CREATE TABLE data (
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
    feat_track_ids TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### Table: `data_reject` (Duplicate Records)
```sql
CREATE TABLE data_reject (
    id SERIAL PRIMARY KEY,
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
    feat_track_ids TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## Logging

The application provides comprehensive logging:
- **Console Output**: Real-time processing information
- **Log File**: `data_cleansing.log` (when running locally)
- **Log Levels**: INFO, ERROR, WARNING

## Error Handling

The application handles various error scenarios:
- Database connection failures
- CSV file reading errors
- Data parsing exceptions
- File creation issues
- Memory constraints

## Monitoring & Debugging

### Checking Database Content

```bash
# Connect to PostgreSQL in Docker
docker-compose exec postgres psql -U postgres -d data_cleansing

# Check table contents
SELECT COUNT(*) FROM data;
SELECT COUNT(*) FROM data_reject;

# View sample records
SELECT * FROM data LIMIT 5;
SELECT * FROM data_reject LIMIT 5;
```

### Viewing Logs

```bash
# View application logs
docker-compose logs data-cleaner

# Follow logs in real-time
docker-compose logs -f data-cleaner

# View PostgreSQL logs
docker-compose logs postgres
```

## Performance Considerations

1. **Memory Usage**: The application loads the entire CSV into memory
2. **Database Performance**: Indexed on key columns for faster queries
3. **Batch Processing**: Uses pandas for efficient data operations
4. **Connection Pooling**: SQLAlchemy handles database connections efficiently

## Possible Improvements

1. **Batch Processing**: For very large CSV files, implement chunked processing
2. **Configuration Management**: Use environment files for different environments
3. **Data Validation**: Add more comprehensive data validation rules
4. **Monitoring**: Add metrics collection and monitoring
5. **Retry Logic**: Implement retry mechanisms for transient failures
6. **Parallel Processing**: Process data in parallel for better performance
7. **Schema Validation**: Validate CSV schema before processing
8. **Incremental Processing**: Support for processing only new records

## Troubleshooting

### Common Issues

1. **Database Connection Failed**
   - Check if PostgreSQL is running
   - Verify database credentials
   - Ensure database exists

2. **CSV File Not Found**
   - Check if `/source/scrap.csv` exists
   - Verify file permissions
   - Ensure correct volume mounting in Docker

3. **Memory Issues**
   - Increase Docker memory limits
   - Consider chunked processing for large files

4. **Permission Errors**
   - Check file/directory permissions
   - Ensure Docker user has access to volumes

### Debug Commands

```bash
# Check Docker containers
docker-compose ps

# Check database connectivity
docker-compose exec postgres pg_isready

# Check file permissions
ls -la source/ target/

# Check application logs
docker-compose logs data-cleaner
```
---

**Note**: This application is designed for the technical test requirements and includes comprehensive error handling, testing, and documentation 