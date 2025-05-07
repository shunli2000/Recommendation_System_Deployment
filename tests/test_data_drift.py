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
    
class TestDriftDetection:
    def test_calculate_psi_normal(self):
        """Test PSI calculation with normal distributions"""
        expected = Counter({'A': 50, 'B': 30, 'C': 20})
        actual = Counter({'A': 45, 'B': 35, 'C': 20})
        
        psi = calculate_psi(expected, actual)
        
        # PSI should be low (distributions are similar)
        assert psi is not None
        assert psi < PSI_THRESHOLD
    
    def test_calculate_psi_different(self):
        """Test PSI calculation with significantly different distributions"""
        expected = Counter({'A': 80, 'B': 15, 'C': 5})
        actual = Counter({'A': 30, 'B': 20, 'C': 50})
        
        psi = calculate_psi(expected, actual)
        
        # PSI should be high (distributions are different)
        assert psi is not None
        assert psi > PSI_THRESHOLD
    
    def test_calculate_psi_empty(self):
        """Test PSI calculation with empty distributions"""
        expected = Counter()
        actual = Counter({'A': 30, 'B': 20, 'C': 50})
        
        psi = calculate_psi(expected, actual)
        
        # PSI should be None for empty distributions
        assert psi is None

    def test_calculate_psi_different_categories(self):
        """Test PSI calculation with different categories"""
        expected = Counter({'A': 50, 'B': 30, 'C': 20})
        actual = Counter({'A': 45, 'D': 35, 'C': 20})
        
        psi = calculate_psi(expected, actual)
        
        # PSI should account for different categories
        assert psi is not None
        assert psi > 0

    def test_detect_ks_drift_similar(self):
        """Test KS test with similar distributions"""
        historical = np.random.normal(100, 15, 1000)
        new = np.random.normal(100, 15, 100)
        
        ks_stat, p_value = detect_ks_drift(historical, new)
        
        # p-value should be high (distributions are similar)
        assert p_value > KS_THRESHOLD
    
    def test_detect_ks_drift_different(self):
        """Test KS test with different distributions"""
        historical = np.random.normal(100, 15, 1000)
        new = np.random.normal(150, 15, 100)  # Different mean
        
        ks_stat, p_value = detect_ks_drift(historical, new)
        
        # p-value should be low (distributions are different)
        assert p_value < KS_THRESHOLD

@patch('kafka_live_consumer.collected_data', {
    "genres": ["Action", "Comedy", "Drama"],
    "gender": ["M", "F", "M"],
    "rating": ["4", "5", "3"],
    "budget": [1000000, 2000000, 3000000],
    "revenue": [3000000, 5000000, 7000000],
    "popularity": [5.6, 7.8, 4.2],
    "age": [25, 32, 45],
    "watch_count": [3, 1, 2]
})
@patch('kafka_live_consumer.event_counter', 1000)
@patch('kafka_live_consumer.batch_size', 1000)
@patch('kafka_live_consumer.historical_genres', Counter({'Action': 100, 'Comedy': 80, 'Drama': 120}))
@patch('kafka_live_consumer.historical_gender', Counter({'M': 150, 'F': 100}))
@patch('kafka_live_consumer.historical_ratings', Counter({'4': 80, '5': 70, '3': 60}))
@patch('kafka_live_consumer.historical_budget', [1000000, 2000000, 3000000, 4000000])
@patch('kafka_live_consumer.historical_revenue', [2000000, 4000000, 6000000, 8000000])
@patch('kafka_live_consumer.historical_popularity', [4.5, 6.7, 8.9, 5.6])
@patch('kafka_live_consumer.historical_age', [22, 28, 35, 42])
@patch('kafka_live_consumer.historical_watch_count', [1, 2, 3, 4])
class TestDriftDetectionIntegration:
    @patch('kafka_live_consumer.calculate_psi')
    @patch('builtins.print')
    def test_check_psi_drift_triggered(self, mock_print, mock_calculate_psi):
        """Test PSI drift detection is triggered and processed properly"""
        # Set up mock to return values indicating drift
        mock_calculate_psi.side_effect = [0.3, 0.1, 0.4]  # genres, gender, rating
        
        check_psi_drift()
        
        # Check that calculate_psi was called for each category
        assert mock_calculate_psi.call_count == 3
        
        # Verify that alerts were printed for genres and rating (which have PSI > THRESHOLD)
        mock_print.assert_any_call("Alert: Significant genre distribution drift detected!")
        mock_print.assert_any_call("Alert: Significant rating distribution drift detected!")
        
        # No alert for gender (PSI < THRESHOLD)
        assert not any("gender distribution drift" in str(c) for c in mock_print.call_args_list)

    @patch('kafka_live_consumer.detect_ks_drift')
    @patch('builtins.print')
    def test_check_ks_drift_triggered(self, mock_print, mock_detect_ks_drift):
        """Test KS drift detection is triggered and processed properly"""
        # Set up mock to return values indicating drift
        mock_detect_ks_drift.side_effect = [
            (0.2, 0.01),  # budget (drift detected)
            (0.1, 0.06),  # revenue (no drift)
            (0.15, 0.04), # popularity (drift detected)
            (0.05, 0.2),  # age (no drift)
            (0.08, 0.08)  # watch_count (no drift)
        ]
        
        check_ks_drift()
        
        # Check that detect_ks_drift was called for each category
        assert mock_detect_ks_drift.call_count == 5
        
        # Verify that alerts were printed for budget and popularity (which have p < THRESHOLD)
        mock_print.assert_any_call("Alert: Significant budget distribution drift detected!")
        mock_print.assert_any_call("Alert: Significant popularity distribution drift detected!")
        
        # No alerts for revenue, age, watch_count (p > THRESHOLD)
        assert not any("revenue distribution drift" in str(c) for c in mock_print.call_args_list)
        assert not any("age distribution drift" in str(c) for c in mock_print.call_args_list)
        assert not any("watch count distribution drift" in str(c) for c in mock_print.call_args_list)