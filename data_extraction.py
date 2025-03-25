#!/usr/bin/env python3
import os
import requests
import pandas as pd
import argparse
import sys
import io
from datetime import datetime
import logging
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Constants
OUTPUT_DIR = "raw_data"
DEFAULT_STATION_IDS = [26953, 31688]
DEFAULT_YEARS = [2023, 2024]
BASE_URL = "https://climate.weather.gc.ca/climate_data/bulk_data_e.html"

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Extract weather data from climate.weather.gc.ca')
    
    parser.add_argument('--stations', nargs='+', type=int, 
                        help='Station IDs to fetch data for (default: 26953 31688)',
                        default=os.environ.get('WEATHER_STATION', '').split() or DEFAULT_STATION_IDS)
    
    parser.add_argument('--years', nargs='+', type=int,
                        help='Years to fetch data for (default: 2023 2024)',
                        default=os.environ.get('WEATHER_YEAR', '').split() or DEFAULT_YEARS)
    
    parser.add_argument('--output-dir', type=str,
                        help=f'Directory to save output files (default: {OUTPUT_DIR})',
                        default=OUTPUT_DIR)
    
    args = parser.parse_args()
    
    # Convert single items to lists if necessary
    if isinstance(args.stations, int):
        args.stations = [args.stations]
    if isinstance(args.years, int):
        args.years = [args.years]
    
    return args

def ensure_output_dir(output_dir):
    """Create output directory if it doesn't exist"""
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        logger.info(f"Created directory: {output_dir}")

def fetch_weather_data(station_id, year, month=1, day=1):
    """
    Fetch weather data from the API
    
    Args:
        station_id (int): Weather station ID
        year (int): Year to get data for
        month (int): Month (default 1)
        day (int): Day (default 1)
        
    Returns:
        pd.DataFrame: DataFrame with the weather data
    """
    logger.info(f"Fetching data for station {station_id}, year {year}, month {month}, day {day}")
    
    params = {
        'format': 'csv',
        'stationID': station_id,
        'Year': year,
        'Month': month,
        'Day': day,
        'time': 'LST',
        'timeframe': 1,
        'submit': 'Download+Data'
    }
    
    # Add retry mechanism
    max_retries = 3
    retry_delay = 2
    
    for attempt in range(max_retries):
        try:
            response = requests.get(BASE_URL, params=params)
            response.raise_for_status()
            
            # Parse CSV data
            data = pd.read_csv(io.StringIO(response.text), skiprows=0)
            return data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching data (attempt {attempt+1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                logger.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                retry_delay *= 2
            else:
                logger.error(f"Failed to fetch data after {max_retries} attempts")
                raise
    
    return None

def check_data_quality(df):
    """
    Perform data quality checks
    
    Args:
        df (pd.DataFrame): Weather data
        
    Returns:
        dict: Dictionary with quality check results
    """
    results = {}
    
    # Check for nulls
    null_counts = df.isnull().sum()
    results['null_counts'] = null_counts[null_counts > 0].to_dict()
    
    # Check for missing days
    if 'Date/Time' in df.columns:
        df['Date'] = pd.to_datetime(df['Date/Time']).dt.date
        date_range = pd.date_range(start=df['Date'].min(), end=df['Date'].max())
        missing_dates = set(date_range.date) - set(df['Date'])
        results['missing_dates'] = list(missing_dates)
    
    # Check for outliers in temperature
    if 'Temp (°C)' in df.columns:
        temp_mean = df['Temp (°C)'].mean()
        temp_std = df['Temp (°C)'].std()
        lower_bound = temp_mean - 3 * temp_std
        upper_bound = temp_mean + 3 * temp_std
        outliers = df[(df['Temp (°C)'] < lower_bound) | (df['Temp (°C)'] > upper_bound)]
        results['temperature_outliers'] = len(outliers)
    
    return results

def main():
    """Main function to extract data"""
    # Parse command line arguments
    args = parse_args()
    
    # Use arguments
    output_dir = args.output_dir
    station_ids = args.stations
    years = args.years
    
    logger.info(f"Running extraction for stations: {station_ids}, years: {years}")
    
    ensure_output_dir(output_dir)
    
    all_quality_results = {}
    
    for station_id in station_ids:
        for year in years:
            try:
                # Fetch data for the entire year
                df = fetch_weather_data(station_id, year)
                
                if df is not None and not df.empty:
                    # Save raw data
                    output_file = f"{output_dir}/station_{station_id}_{year}.csv"
                    df.to_csv(output_file, index=False)
                    logger.info(f"Saved data to {output_file}")
                    
                    # Perform data quality checks
                    quality_results = check_data_quality(df)
                    all_quality_results[f"station_{station_id}_{year}"] = quality_results
                    
                    if quality_results['null_counts']:
                        logger.warning(f"Found null values in station {station_id}, year {year}: {quality_results['null_counts']}")
                    
                    if 'missing_dates' in quality_results and quality_results['missing_dates']:
                        logger.warning(f"Found {len(quality_results['missing_dates'])} missing dates in station {station_id}, year {year}")
                    
                    if 'temperature_outliers' in quality_results and quality_results['temperature_outliers'] > 0:
                        logger.warning(f"Found {quality_results['temperature_outliers']} temperature outliers in station {station_id}, year {year}")
                
                # Add a small delay to avoid overwhelming the API
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"Error processing station {station_id}, year {year}: {e}")
    
    # Save data quality results
    quality_file = f"{output_dir}/data_quality_report.json"
    pd.DataFrame(all_quality_results).to_json(quality_file)
    logger.info(f"Saved data quality report to {quality_file}")

if __name__ == "__main__":
    main() 