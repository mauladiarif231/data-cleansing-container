# CSV Data Cleansing Application & Pipeline

## Overview

This project provides a robust solution for cleaning CSV data, removing duplicate records based on unique IDs, transforming data into proper formats, and storing results in a PostgreSQL database. It includes two implementations:

1. **Standalone Application**: A containerized Python application for one-time CSV data cleansing, with backup file generation and comprehensive error handling.
2. **Airflow Pipeline**: An automated, scheduled data cleansing pipeline using Apache Airflow for hourly processing, with advanced monitoring, data quality checks, and file management.

Both implementations deduplicate data based on the `ids` column, store clean and duplicate records in separate database tables, and generate backup files in CSV and JSON formats.

## Features

### Shared Features
- **Data Deduplication**: Removes duplicates based on the `ids` column.
- **Data Transformation**: Converts data types and formats (e.g., dates to YYYY-MM-DD, names to uppercase, genres to lists).
- **Database Integration**: Stores clean data in the `data` table and duplicates in the `data_reject` table in PostgreSQL.
- **Backup Generation**: Creates CSV files for duplicates and JSON files for clean data.
- **Error Handling**: Comprehensive logging and exception handling.
- **Containerization**: Fully containerized with Docker and Docker Compose.
- **Testing**: Unit tests using pytest with coverage reporting.

### Standalone Application Features
- Simple execution for one-time data processing.
- Lightweight setup for quick deployment.

### Airflow Pipeline Features
- **Automated Scheduling**: Hourly execution (every hour at 00 minutes).
- **File Change Detection**: Uses MD5 hashing to prevent duplicate processing.
- **Data Quality & Integrity**: Validates data types, output files, and record counts.
- **Monitoring & Metrics**: Collects pipeline metrics (e.g., processing time, success rates) and provides health checks.
- **Data Management**: Automatic backups, archiving with a 7-day retention policy, and cleanup of old files.
- **Advanced Logging**: Detailed error logs stored in a dedicated database table.

## Project Structure

### Standalone Application
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

### Airflow Pipeline
```
├── dags/
│   └── csv_data_cleansing_pipeline.py    
├── source/
│   └── scrap.csv
├── target/          
├── backup/ 
├── archive/ 
├── logs/
├── main.py 
├── ddl.sql 
├── docker-compose.yaml 
├── Dockerfile 
├── requirements.txt 
├── deploy.sh  
├── init-databases.sh 
└── pytest.ini 
```

## Requirements

### System Requirements
- Docker and Docker Compose
- Python 3.11+ (if running locally)
- PostgreSQL (if running locally)
- At least 4GB RAM and 2GB free disk space (for Airflow Pipeline)

### Python Dependencies
- pandas
- psycopg2-binary==2.9.9
- python-dotenv==1.0.0
- pytest==7.4.3
- pytest-cov==4.1.0
- sqlalchemy==2.0.23
- apache-airflow (for Airflow Pipeline)

See `requirements.txt` for the complete list.

## Database Schema

### Shared Tables
- **`data`**: Stores clean, deduplicated records with `ids` as the primary key.
- **`data_reject`**: Stores duplicate records with an auto-incrementing `id`.

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

### Airflow Pipeline Additional Tables
- **`file_processing_log`**: Tracks processed files using MD5 hashes.
- **`error_log`**: Stores pipeline errors and exceptions.
- **`pipeline_metrics`**: Records performance and quality metrics.

## Installation & Setup

### Standalone Application

#### Method 1: Using Docker Compose (Recommended)
1. **Prepare the project files**:
   ```bash
   # Place your source CSV file in ./source/scrap.csv
   ```

2. **Build and run**:
   ```bash
   docker-compose up --build
   # Or run in detached mode
   docker-compose up --build -d
   ```

3. **Check logs**:
   ```bash
   docker-compose logs data-cleaner
   ```

4. **Stop services**:
   ```bash
   docker-compose down
   ```

#### Method 2: Local Installation
1. **Install PostgreSQL**:
   ```bash
   # Ubuntu/Debian
   sudo apt-get install postgresql postgresql-contrib
   # macOS
   brew install postgresql
   ```

2. **Create database**:
   ```bash
   sudo -u postgres psql
   CREATE DATABASE data_cleansing;
   CREATE USER postgres WITH PASSWORD 'password';
   GRANT ALL PRIVILEGES ON DATABASE data_cleansing TO postgres;
   \q
   ```

3. **Install Python dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

4. **Run the application**:
   ```bash
   python main.py
   ```

### Airflow Pipeline

#### Quick Start
1. **Clone and prepare the project**:
   ```bash
   git clone <repository>
   cd data-cleansing-pipeline
   ```

2. **Place your CSV file**:
   ```bash
   cp scrap.csv source/scrap.csv
   ```

3. **Deploy the pipeline**:
   ```bash
   chmod +x deploy.sh
   ./deploy.sh
   # Or manually
   docker-compose down -v
   docker-compose up --build -d
   docker build --no-cache -t data-cleaner:latest .
   ```

4. **Access services**:
   - Airflow UI: http://localhost:8080 (admin/admin)
   - PgAdmin: http://localhost:8081 (admin@example.com/admin)

#### Manual Execution (Without Airflow)
```bash
docker run --rm \
  --network data-network \
  -v $(pwd)/source:/source:ro \
  -v $(pwd)/target:/target \
  -v $(pwd)/logs:/app/logs \
  -e DB_HOST=postgres \
  -e DB_PORT=5432 \
  -e DB_NAME=data_cleansing \
  -e DB_USER=postgres \
  -e DB_PASSWORD=password \
  data-cleaner:latest
```

## Configuration

### Environment Variables
```bash
DB_HOST=postgres
DB_PORT=5432
DB_NAME=data_cleansing
DB_USER=postgres
DB_PASSWORD=password
EXECUTION_DATE=2025-06-14  # For Airflow Pipeline
```

### Docker Compose Services (Airflow Pipeline)
- **postgres**: PostgreSQL database
- **airflow-init**: Airflow database initialization
- **airflow-webserver**: Airflow web interface
- **airflow-scheduler**: Airflow task scheduler
- **data-cleaner**: Data processing container
- **pgadmin**: Database administration interface

## Input/Output

### Input
- **File**: `/source/scrap.csv`
- **Format**: CSV with the following columns:
  - dates, ids, names, monthly_listeners, popularity, followers, genres, first_release, last_release, num_releases, num_tracks, playlists_found, feat_track_ids

### Output

#### Database Tables
- **`data`**: Clean records with unique IDs.
- **`data_reject`**: Duplicate records.
- Airflow Pipeline additional tables: `file_processing_log`, `error_log`, `pipeline_metrics`.

#### Backup Files
- **`/target/data_YYYYMMDDHHMMSS.json`**: Clean records in JSON format.
- **`/target/data_reject_YYYYMMDDHHMMSS.csv`**: Duplicate records in CSV format.
- Airflow Pipeline additional directories: `/backup/` (source file backups), `/archive/` (processed files).

### JSON Output Format
```json
{
  "row_count": 100,
  "data": [
    {
      "dates": "2025-06-14",
      "ids": "unique_id_123",
      "names": "ARTIST NAME",
      "monthly_listeners": 1000000,
      "popularity": 85,
      "followers": 500000,
      "genres": ["pop", "rock"],
      "first_release": "2020",
      "last_release": "2024",
      "num_releases": 5,
      "num_tracks": 50,
      "playlists_found": "popular_playlists",
      "feat_track_ids": ["track1", "track2"]
    }
  ]
}
```

## Data Processing Rules

1. **Deduplication**: Identifies and separates records with duplicate `ids`.
2. **Data Transformation**:
   - `dates`: Converted to YYYY-MM-DD format.
   - `names`: Converted to uppercase.
   - `genres`: Parsed as a list of strings.
   - `feat_track_ids`: Parsed as a list of strings.
   - Numeric fields: Converted to integer/bigint types.
3. **Database Storage**:
   - Clean records: Stored in `data` table with `ids` as primary key.
   - Duplicate records: Stored in `data_reject` table with auto-increment ID.

## Airflow Pipeline Tasks

1. **File Detection**: Monitors `/source/scrap.csv` availability.
2. **Database Validation**: Verifies PostgreSQL connection.
3. **Source Backup**: Creates timestamped backups in `/backup/`.
4. **Change Detection**: Uses MD5 hashing to skip unchanged files.
5. **Data Processing**: Runs data cleansing and stores results.
6. **Output Validation**: Verifies JSON and CSV output files.
7. **Quality Checks**: Validates record counts and data integrity.
8. **Metrics Collection**: Records performance metrics.
9. **File Archiving**: Moves output files to `/archive/`.
10. **Cleanup**: Removes files older than 7 days.

## Testing

### Running Unit Tests
```bash
pip install pytest pytest-cov
pytest -v
pytest --cov=main  # With coverage
```

### Airflow Pipeline Integration Tests
```bash
docker-compose exec airflow-scheduler airflow dags test csv_data_cleansing_pipeline $(date +%Y-%m-%d)
```

### Test Coverage
- CSV reading and data cleaning logic
- Database connection handling
- File creation operations
- Error handling scenarios
- Pipeline task execution (Airflow)

## Monitoring & Troubleshooting

### Standalone Application
```bash
# Check database content
docker-compose exec postgres psql -U postgres -d data_cleansing
SELECT COUNT(*) FROM data;
SELECT COUNT(*) FROM data_reject;

# View logs
docker-compose logs -f data-cleaner
```

### Airflow Pipeline
```bash
# Check service status
docker-compose ps

# View logs
docker-compose logs -f airflow-scheduler
docker-compose logs -f data-cleaner

# Database monitoring
SELECT * FROM pipeline_metrics ORDER BY created_at DESC LIMIT 10;
SELECT * FROM error_log ORDER BY created_at DESC;
SELECT * FROM file_processing_log ORDER BY created_at DESC;
```

### Common Issues
1. **Database Connection Failed**:
   - Verify PostgreSQL is running and credentials are correct.
2. **CSV File Not Found**:
   - Ensure `/source/scrap.csv` exists with correct permissions.
3. **Memory Issues**:
   - Increase Docker memory limits or enable chunked processing.
4. **Duplicate Processing (Airflow)**:
   - Check `file_processing_log` for MD5 hash issues.

## Performance Considerations

- **Memory Usage**: Loads entire CSV into memory (consider chunked processing for large files).
- **Database Performance**: Indexes on key columns for faster queries.
- **Batch Processing**: Uses pandas for efficient operations.
- **Connection Pooling**: SQLAlchemy manages database connections.
- Airflow Pipeline: Optimized with database indexing, connection pooling, and memory-mapped file operations.

## Security Considerations (Airflow Pipeline)
- Database credentials stored in environment variables.
- Non-root user in data processing container.
- Read-only mounts for source files.
- Network isolation using Docker networks.

## Possible Improvements

### Shared Improvements
1. **Batch Processing**: Implement chunked processing for large CSV files.
2. **Configuration Management**: Use environment files for different environments.
3. **Data Validation**: Add comprehensive schema validation.
4. **Monitoring**: Integrate with Prometheus/Grafana.
5. **Retry Logic**: Implement retries for transient failures.

### Airflow Pipeline Specific
1. **Scalability**: Add Kubernetes support or distributed processing with Spark.
2. **Security**: Implement secret management with Vault and RBAC.
3. **CI/CD**: Automate testing and deployment with Terraform.
4. **Data Validation**: Integrate Great Expectations for schema validation.