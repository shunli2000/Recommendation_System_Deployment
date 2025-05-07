import os
import sys
from unittest.mock import patch
import numpy as np
from collections import Counter

np.random.seed(42)

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.dirname(os.path.dirname(__file__))))

# Import the functions for testing
# Note: We need to mock the imports first since they would otherwise fail
with patch('psycopg2.connect'), patch('kafka.KafkaConsumer'):
    from data_pipeline.kafka_live_consumer import (
        calculate_psi, detect_ks_drift, validate_movie_schema, validate_user_schema,
        check_psi_drift, check_ks_drift, PSI_THRESHOLD, KS_THRESHOLD
    )

class TestSchemaValidation:
    def test_validate_movie_schema_valid(self):
        """Test validation of a valid movie schema"""
        movie_info = {
            'title': 'Test Movie',
            'adult': False,
            'genres': [{'id': 28, 'name': 'Action'}, {'id': 35, 'name': 'Comedy'}],
            'release_date': '2023-01-01',
            'budget': 1000000,
            'revenue': 2000000,
            'popularity': 7.8,
            'runtime': 120
        }
        
        assert validate_movie_schema(movie_info) is True
    
    def test_validate_movie_schema_missing_fields(self):
        """Test validation of a movie schema with missing fields"""
        movie_info = {
            'title': 'Test Movie',
            'adult': False,
            'genres': [{'id': 28, 'name': 'Action'}, {'id': 35, 'name': 'Comedy'}],
            'release_date': '2023-01-01',
            # Missing budget, revenue, popularity, runtime
        }
        
        assert validate_movie_schema(movie_info) is False
    
    def test_validate_movie_schema_null_fields(self):
        """Test validation of a movie schema with null fields"""
        movie_info = {
            'title': 'Test Movie',
            'adult': False,
            'genres': [{'id': 28, 'name': 'Action'}, {'id': 35, 'name': 'Comedy'}],
            'release_date': '2023-01-01',
            'budget': None,  # Null field
            'revenue': 2000000,
            'popularity': 7.8,
            'runtime': 120
        }
        
        assert validate_movie_schema(movie_info) is False
    
    def test_validate_user_schema_valid(self):
        """Test validation of a valid user schema"""
        user_info = {
            'age': 30,
            'occupation': 'Engineer',
            'gender': 'M'
        }
        
        assert validate_user_schema(user_info) is True
    
    def test_validate_user_schema_missing_fields(self):
        """Test validation of a user schema with missing fields"""
        user_info = {
            'age': 30,
            # Missing occupation and gender
        }
        
        assert validate_user_schema(user_info) is False
    
    def test_validate_user_schema_null_fields(self):
        """Test validation of a user schema with null fields"""
        user_info = {
            'age': 30,
            'occupation': None,  # Null field
            'gender': 'M'
        }
        
        assert validate_user_schema(user_info) is False