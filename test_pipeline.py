#!/usr/bin/env python3
import unittest
import pandas as pd
import numpy as np
import os
import tempfile
import json
from unittest.mock import patch, MagicMock

# Import functions from our modules
from data_extraction import fetch_weather_data, check_data_quality
from data_transformation import transform_weather_data, calculate_yoy_delta
from data_analysis import load_data_to_sqlite, run_sql_queries

class TestDataExtraction(unittest.TestCase):
    """Test cases for data extraction functions"""
    
    @patch('data_extraction.requests.get')
    def test_fetch_weather_data(self, mock_get):
        """Test the fetch_weather_data function"""
        # Mock the response
        mock_response = MagicMock()
        mock_response.text = """
        Date/Time,Year,Month,Day,Time,Temp (°C),Dew Point Temp (°C),Rel Hum (%),Wind Dir (10s deg),Wind Spd (km/h),Visibility (km),Stn Press (kPa),Weather
        2023-01-01 00:00,2023,1,1,00:00,5.2,2.1,80,6,10,15,101.2,Cloudy
        2023-01-01 01:00,2023,1,1,01:00,4.8,1.9,82,7,12,14,101.3,Cloudy
        """
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response
        
        # Call the function
        result = fetch_weather_data(12345, 2023)
        
        # Check the results
        self.assertIsNotNone(result)
        self.assertEqual(len(result), 2)  # Two rows in the mocked CSV
        self.assertTrue('Temp (°C)' in result.columns)
        
        # Verify the mock was called with the expected parameters
        mock_get.assert_called_once()
        args, kwargs = mock_get.call_args
        self.assertEqual(kwargs['params']['stationID'], 12345)
        self.assertEqual(kwargs['params']['Year'], 2023)
    
    def test_check_data_quality(self):
        """Test the check_data_quality function"""
        # Create a test DataFrame
        df = pd.DataFrame({
            'Date/Time': ['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-05'],
            'Temp (°C)': [5.2, 4.8, np.nan, 100.0]  # Include null and outlier
        })
        
        # Call the function
        results = check_data_quality(df)
        
        # Check the results
        self.assertIn('null_counts', results)
        self.assertIn('Temp (°C)', results['null_counts'])
        self.assertEqual(results['null_counts']['Temp (°C)'], 1)  # One null value
        
        self.assertIn('missing_dates', results)
        self.assertEqual(len(results['missing_dates']), 1)  # One missing date (Jan 4)
        
        self.assertIn('temperature_outliers', results)
        self.assertGreater(results['temperature_outliers'], 0)  # At least one outlier (100.0)


class TestDataTransformation(unittest.TestCase):
    """Test cases for data transformation functions"""
    
    def test_transform_weather_data(self):
        """Test the transform_weather_data function"""
        # Create a test DataFrame
        df = pd.DataFrame({
            'Date/Time': ['2023-01-01', '2023-01-02', '2023-02-01', '2023-02-02'],
            'Temp (°C)': [5.2, 4.8, 6.1, 5.9],
            'station_id': ['12345', '12345', '12345', '12345']
        })
        
        # Call the function
        result = transform_weather_data(df)
        
        # Check the results
        self.assertIsNotNone(result)
        self.assertEqual(len(result), 2)  # Two months (Jan and Feb)
        self.assertIn('temperature_celsius_avg', result.columns)
        self.assertIn('temperature_celsius_min', result.columns)
        self.assertIn('temperature_celsius_max', result.columns)
        
        # Check the aggregation for January
        jan_row = result[result['month'] == 1].iloc[0]
        self.assertAlmostEqual(jan_row['temperature_celsius_avg'], 5.0)
        self.assertAlmostEqual(jan_row['temperature_celsius_min'], 4.8)
        self.assertAlmostEqual(jan_row['temperature_celsius_max'], 5.2)
    
    def test_calculate_yoy_delta(self):
        """Test the calculate_yoy_delta function"""
        # Create a test DataFrame with data from two years
        df = pd.DataFrame({
            'station_id': ['12345', '12345', '12345', '12345'],
            'year': [2022, 2023, 2022, 2023],
            'month': [1, 1, 2, 2],
            'temperature_celsius_avg': [4.0, 5.0, 5.0, 6.0]
        })
        
        # Call the function
        result = calculate_yoy_delta(df)
        
        # Check the results
        self.assertIsNotNone(result)
        self.assertIn('temperature_celsius_yoy_avg', result.columns)
        
        # The YoY delta for 2023-01 should be 1.0 (5.0 - 4.0)
        yoy_jan_2023 = result[(result['year'] == 2023) & (result['month'] == 1)].iloc[0]
        self.assertAlmostEqual(yoy_jan_2023['temperature_celsius_yoy_avg'], 1.0)
        
        # The YoY delta for 2023-02 should be 1.0 (6.0 - 5.0)
        yoy_feb_2023 = result[(result['year'] == 2023) & (result['month'] == 2)].iloc[0]
        self.assertAlmostEqual(yoy_feb_2023['temperature_celsius_yoy_avg'], 1.0)


class TestDataAnalysis(unittest.TestCase):
    """Test cases for data analysis functions"""
    
    def test_load_data_to_sqlite(self):
        """Test loading data to SQLite"""
        # Create a temporary directory and file
        with tempfile.TemporaryDirectory() as tmpdirname:
            # Create a test CSV file
            test_df = pd.DataFrame({
                'station_name': ['Station A', 'Station A', 'Station B'],
                'climate_id': ['12345', '12345', '67890'],
                'date_month': ['2023-01', '2023-02', '2023-01'],
                'temperature_celsius_avg': [5.0, 6.0, 4.0]
            })
            
            # Save to a temporary CSV file
            test_csv = os.path.join(tmpdirname, 'test.csv')
            test_df.to_csv(test_csv, index=False)
            
            # Create a temporary database file
            test_db = os.path.join(tmpdirname, 'test.db')
            
            # Call the function
            conn = load_data_to_sqlite(test_csv, test_db)
            
            # Query the database to verify the data was loaded
            result = pd.read_sql_query("SELECT * FROM weather_data", conn)
            
            # Check the results
            self.assertEqual(len(result), 3)  # Three rows in the test data
            self.assertIn('station_name', result.columns)
            self.assertIn('temperature_celsius_avg', result.columns)
            
            # Clean up
            conn.close()

    @patch('data_analysis.pd.read_sql_query')
    def test_run_sql_queries(self, mock_read_sql):
        """Test running SQL queries"""
        # Mock the SQL results
        mock_read_sql.return_value = pd.DataFrame({
            'station_name': ['Station A'],
            'year': ['2023'],
            'avg_temperature': [5.5]
        })
        
        # Create a mock connection
        mock_conn = MagicMock()
        
        # Call the function
        results = run_sql_queries(mock_conn)
        
        # Check the results
        self.assertIn('avg_temp_by_station_year', results)
        self.assertEqual(len(results), 5)  # Five queries
        
        # Verify the mock was called for each query
        self.assertEqual(mock_read_sql.call_count, 5)


if __name__ == '__main__':
    unittest.main() 