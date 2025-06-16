import pytest
import pandas as pd
import os
import json
from unittest.mock import Mock, patch, MagicMock
from main import DataCleaner

class TestDataCleaner:
    
    @pytest.fixture
    def db_config(self):
        """Sample database configuration for testing"""
        return {
            'host': 'localhost',
            'port': 5432,
            'database': 'test_db',
            'user': 'test_user',
            'password': 'test_password'
        }
    
    @pytest.fixture
    def sample_df(self):
        """Sample DataFrame for testing"""
        return pd.DataFrame({
            'dates': ['2024-01-01', '2024-01-02', '2024-01-01'],
            'ids': ['1', '2', '1'],  # Note: duplicate id '1'
            'names': ['artist one', 'artist two', 'artist one'],
            'monthly_listeners': [1000000, 2000000, 1000000],
            'popularity': [80, 90, 80],
            'followers': [500000, 1000000, 500000],
            'genres': ["['pop']", "['rock']", "['pop']"],
            'first_release': ['2020', '2019', '2020'],
            'last_release': ['2024', '2024', '2024'],
            'num_releases': [5, 8, 5],
            'num_tracks': [50, 80, 50],
            'playlists_found': ['100', '200', '100'],
            'feat_track_ids': ["['track1']", "['track2']", "['track1']"]
        })
    
    @pytest.fixture
    def data_cleaner(self, db_config):
        """Create DataCleaner instance for testing"""
        return DataCleaner(db_config)
    
    def test_init(self, data_cleaner, db_config):
        """Test DataCleaner initialization"""
        assert data_cleaner.db_config == db_config
        assert data_cleaner.engine is None
        assert data_cleaner.connection is None
        assert len(data_cleaner.test_datetime) == 14  # YYYYMMDDHHMMSS format
    
    @patch('main.pd.read_csv')
    def test_read_csv_success(self, mock_read_csv, data_cleaner, sample_df):
        """Test successful CSV reading"""
        mock_read_csv.return_value = sample_df
        
        result = data_cleaner.read_csv('/test/path.csv')
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 3
        mock_read_csv.assert_called_once_with('/test/path.csv')
    
    @patch('main.pd.read_csv')
    def test_read_csv_success(self, mock_read_csv, data_cleaner, sample_df):
        """Test successful CSV reading"""
        mock_read_csv.return_value = sample_df

        result = data_cleaner.read_csv('/test/path.csv')

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 3
        mock_read_csv.assert_called_once_with('/test/path.csv', encoding='utf-8')
    
    def test_clean_data(self, data_cleaner, sample_df):
        """Test data cleaning functionality"""
        clean_data, duplicate_data = data_cleaner.clean_data(sample_df)
        
        # Should have 2 clean records and 1 duplicate
        assert len(clean_data) == 2
        assert len(duplicate_data) == 1
        
        # Check that names are uppercase
        assert all(clean_data['names'].str.isupper())
        assert all(duplicate_data['names'].str.isupper())
        
        # Check unique ids in clean data
        assert len(clean_data['ids'].unique()) == len(clean_data)
        
        # Check that duplicate has the repeated id
        assert duplicate_data.iloc[0]['ids'] == '1'
    
    def test_clean_data_no_duplicates(self, data_cleaner):
        """Test cleaning data with no duplicates"""
        df = pd.DataFrame({
            'dates': ['2024-01-01', '2024-01-02'],
            'ids': ['1', '2'],
            'names': ['artist one', 'artist two'],
            'monthly_listeners': [1000000, 2000000],
            'popularity': [80, 90],
            'followers': [500000, 1000000],
            'genres': ["['pop']", "['rock']"],
            'first_release': ['2020', '2019'],
            'last_release': ['2024', '2024'],
            'num_releases': [5, 8],
            'num_tracks': [50, 80],
            'playlists_found': ['100', '200'],
            'feat_track_ids': ["['track1']", "['track2']"]
        })
        
        clean_data, duplicate_data = data_cleaner.clean_data(df)
        
        assert len(clean_data) == 2
        assert len(duplicate_data) == 0
    
    @patch('main.os.makedirs')
    @patch('builtins.open', new_callable=MagicMock)
    def test_create_backup_files(self, mock_open, mock_makedirs, data_cleaner, sample_df):
        """Test backup file creation"""
        clean_data, duplicate_data = data_cleaner.clean_data(sample_df)
        
        # Mock file operations
        mock_file = MagicMock()
        mock_open.return_value.__enter__.return_value = mock_file
        
        # Mock pandas to_csv
        with patch.object(pd.DataFrame, 'to_csv') as mock_to_csv:
            data_cleaner.create_backup_files(clean_data, duplicate_data)
            
            # Check that directories are created
            mock_makedirs.assert_called_with(os.path.join('target'), exist_ok=True)
            
            # Check that CSV file is created
            mock_to_csv.assert_called_once()
            
            # Check that JSON file is opened for writing
            mock_open.assert_called()
    
    @patch('main.psycopg2.connect')
    @patch('main.create_engine')
    def test_connect_db_success(self, mock_create_engine, mock_connect, data_cleaner):
        """Test successful database connection"""
        mock_engine = Mock()
        mock_connection = Mock()
        mock_create_engine.return_value = mock_engine
        mock_connect.return_value = mock_connection
        
        data_cleaner.connect_db()
        
        assert data_cleaner.engine == mock_engine
        assert data_cleaner.connection == mock_connection
    
    @patch('main.psycopg2.connect')
    def test_connect_db_failure(self, mock_connect, data_cleaner):
        """Test database connection failure"""
        mock_connect.side_effect = Exception("Connection failed")
        
        with pytest.raises(Exception):
            data_cleaner.connect_db()
    
    def test_close_connections(self, data_cleaner):
        """Test closing database connections"""
        # Mock connections
        mock_connection = Mock()
        mock_engine = Mock()
        data_cleaner.connection = mock_connection
        data_cleaner.engine = mock_engine
        
        data_cleaner.close_connections()
        
        mock_connection.close.assert_called_once()
        mock_engine.dispose.assert_called_once()

class TestDataCleanerIntegration:
    """Integration tests that require more setup"""
    
    @pytest.fixture
    def sample_df(self):
        """Sample DataFrame for testing"""
        return pd.DataFrame({
            'dates': ['2024-01-01', '2024-01-02', '2024-01-01'],
            'ids': ['1', '2', '1'],  # Note: duplicate id '1'
            'names': ['artist one', 'artist two', 'artist one'],
            'monthly_listeners': [1000000, 2000000, 1000000],
            'popularity': [80, 90, 80],
            'followers': [500000, 1000000, 500000],
            'genres': ["['pop']", "['rock']", "['pop']"],
            'first_release': ['2020', '2019', '2020'],
            'last_release': ['2024', '2024', '2024'],
            'num_releases': [5, 8, 5],
            'num_tracks': [50, 80, 50],
            'playlists_found': ['100', '200', '100'],  # Changed to strings
            'feat_track_ids': ["['track1']", "['track2']", "['track1']"]
        })

    @pytest.fixture
    def temp_csv_file(self, tmp_path):
        """Create a temporary CSV file for testing"""
        csv_content = """dates,ids,names,monthly_listeners,popularity,followers,genres,first_release,last_release,num_releases,num_tracks,playlists_found,feat_track_ids
    2024-01-01,1,artist one,1000000,80,500000,"['pop']",2020,2024,5,50,"100","['track1']"
    2024-01-02,2,artist two,2000000,90,1000000,"['rock']",2019,2024,8,80,"200","['track2']"
    2024-01-01,1,artist one,1000000,80,500000,"['pop']",2020,2024,5,50,"100","['track1']"
    """
        csv_file = tmp_path / "test.csv"
        csv_file.write_text(csv_content)
        return str(csv_file)
    
    def test_read_and_clean_integration(self, temp_csv_file):
        """Integration test for reading and cleaning CSV"""
        db_config = {
            'host': 'localhost',
            'port': 5432,
            'database': 'test_db',
            'user': 'test_user',
            'password': 'test_password'
        }
        
        cleaner = DataCleaner(db_config)
        
        # Read CSV
        df = cleaner.read_csv(temp_csv_file)
        assert len(df) == 3
        
        # Clean data
        clean_data, duplicate_data = cleaner.clean_data(df)
        assert len(clean_data) == 2
        assert len(duplicate_data) == 1

if __name__ == "__main__":
    pytest.main([__file__, "-v"])