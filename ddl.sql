-- DDL Script for Data Cleansing Tables
-- Database: PostgreSQL

-- Create database if not exists
-- CREATE DATABASE data_cleansing;

-- Use the database
-- \c data_cleansing;

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
    feat_track_ids TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create data_reject table for duplicate records
CREATE TABLE IF NOT EXISTS data_reject (
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

CREATE TABLE IF NOT EXISTS file_processing_log (
    id SERIAL PRIMARY KEY,
    file_hash VARCHAR(32) UNIQUE,
    file_path VARCHAR(500),
    processing_status VARCHAR(50) DEFAULT 'processed',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS error_log (
    id SERIAL PRIMARY KEY,
    dag_id VARCHAR(255),
    execution_date TIMESTAMP,
    task_id VARCHAR(255),
    error_message TEXT,
    log_url TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS pipeline_metrics (
    id SERIAL PRIMARY KEY,
    execution_date DATE,
    total_records INTEGER,
    clean_records INTEGER,
    rejected_records INTEGER,
    processing_time FLOAT,
    file_size BIGINT,
    memory_usage FLOAT,
    success_rate FLOAT,
    dag_run_id VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_data_dates ON data(dates);
CREATE INDEX IF NOT EXISTS idx_data_names ON data(names);
CREATE INDEX IF NOT EXISTS idx_data_reject_ids ON data_reject(ids);
CREATE INDEX IF NOT EXISTS idx_data_reject_dates ON data_reject(dates);

-- Add comments to tables
COMMENT ON TABLE data IS 'Table containing cleaned unique records';
COMMENT ON TABLE data_reject IS 'Table containing duplicate records that were rejected';

-- Grant permissions (adjust as needed)
GRANT SELECT, INSERT, UPDATE, DELETE ON data TO postgres;
GRANT SELECT, INSERT, UPDATE, DELETE ON data_reject TO postgres;
GRANT USAGE, SELECT ON SEQUENCE data_reject_id_seq TO postgres;