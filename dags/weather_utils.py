#!/usr/bin/env python3
import os
import requests
import pandas as pd
import numpy as np
import glob
import logging
import time
import io
import sqlite3
from datetime import datetime
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Constants
RAW_DATA_DIR = "raw_data"
OUTPUT_DIR = "output"
GEONAMES_FILE = "geonames.csv"
DEFAULT_DB_PATH = "weather_data.db"
BASE_URL = "https://climate.weather.gc.ca/climate_data/bulk_data_e.html"

def ensure_dir(directory):
    """Create directory if it doesn't exist"""
    if not os.path.exists(directory):
        os.makedirs(directory)
        logger.info(f"Created directory: {directory}")

def fetch_weather_data(station_id, year, month=1, day=1, output_dir=RAW_DATA_DIR):
    """
    Fetch weather data from the API and save to CSV
    
    Args:
        station_id (int): Weather station ID
        year (int): Year to get data for
        month (int): Month (default 1)
        day (int): Day (default 1)
        output_dir (str): Directory to save output file
        
    Returns:
        str: Path to saved file
        dict: Data quality results
    """
    logger.info(f"Fetching data for station {station_id}, year {year}, month {month}, day {day}")
    
    # Ensure output directory exists
    ensure_dir(output_dir)
    
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
            df = pd.read_csv(io.StringIO(response.text), skiprows=0)
            
            # Check data quality
            quality_results = check_data_quality(df, station_id, year)
            
            # Save data to file
            output_file = f"{output_dir}/station_{station_id}_{year}.csv"
            df.to_csv(output_file, index=False)
            logger.info(f"Saved data to {output_file}")
            
            # Log quality issues
            if quality_results['null_counts']:
                logger.warning(f"Found null values in station {station_id}, year {year}: {quality_results['null_counts']}")
            
            return output_file, quality_results
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching data (attempt {attempt+1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                logger.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                retry_delay *= 2
            else:
                logger.error(f"Failed to fetch data after {max_retries} attempts")
                raise
    
    return None, None

def check_data_quality(df, station_id, year):
    """
    Perform data quality checks
    
    Args:
        df (pd.DataFrame): Weather data
        station_id (int): Station ID
        year (int): Year
        
    Returns:
        dict: Dictionary with quality check results
    """
    quality_report = {
        'station_id': station_id,
        'year': year,
        'record_count': len(df),
        'null_counts': {},
        'missing_dates': [],
        'temperature_outliers': 0
    }
    
    # Check for nulls
    null_counts = df.isnull().sum()
    quality_report['null_counts'] = null_counts[null_counts > 0].to_dict()
    
    # Check for missing days
    datetime_column = None
    if 'Date/Time (LST)' in df.columns:
        datetime_column = 'Date/Time (LST)'
    elif 'Date/Time' in df.columns:
        datetime_column = 'Date/Time'
        
    if datetime_column:
        df['Date'] = pd.to_datetime(df[datetime_column]).dt.date
        date_range = pd.date_range(start=df['Date'].min(), end=df['Date'].max())
        missing_dates = set(date_range.date) - set(df['Date'])
        quality_report['missing_dates'] = list(map(str, missing_dates))
    
    # Check for outliers in temperature
    temp_column = None
    if 'Temp (°C)' in df.columns:
        temp_column = 'Temp (°C)'
    elif 'Mean Temp (°C)' in df.columns:
        temp_column = 'Mean Temp (°C)'
        
    if temp_column and not df[temp_column].isnull().all():
        temp_data = df[temp_column].dropna()
        if len(temp_data) > 0:
            temp_mean = temp_data.mean()
            temp_std = temp_data.std()
            lower_bound = temp_mean - 3 * temp_std
            upper_bound = temp_mean + 3 * temp_std
            outliers = df[(df[temp_column] < lower_bound) | (df[temp_column] > upper_bound)]
            quality_report['temperature_outliers'] = len(outliers)
    
    return quality_report

def save_quality_report(quality_results, output_dir=RAW_DATA_DIR):
    """
    Save data quality report to JSON file
    
    Args:
        quality_results (dict): Dictionary of quality results
        output_dir (str): Directory to save report
        
    Returns:
        str: Path to quality report file
    """
    # Ensure output directory exists
    ensure_dir(output_dir)
    
    quality_file = f"{output_dir}/data_quality_report.json"
    pd.DataFrame(quality_results).to_json(quality_file)
    logger.info(f"Saved data quality report to {quality_file}")
    
    return quality_file

def load_geonames_data(geonames_file=GEONAMES_FILE):
    """
    Load and process the geonames dimension table
    
    Args:
        geonames_file (str): Path to the geonames CSV file
        
    Returns:
        pd.DataFrame: Processed geonames data
    """
    logger.info(f"Loading geonames data from {geonames_file}")
    
    try:
        # Load the geonames CSV file
        geonames_df = pd.read_csv(geonames_file)
        
        # Clean up column names if needed
        geonames_df.columns = [col.strip() for col in geonames_df.columns]
        
        # Extract relevant columns
        relevant_columns = [
            'id', 'name', 'feature.id', 'latitude', 'longitude', 'map'
        ]
        
        # Ensure all required columns exist
        for col in relevant_columns:
            if col not in geonames_df.columns:
                logger.error(f"Required column '{col}' not found in geonames data")
                available_cols = ', '.join(geonames_df.columns)
                logger.info(f"Available columns: {available_cols}")
                raise ValueError(f"Required column '{col}' not found in geonames data")
        
        # Select only relevant columns
        cleaned_df = geonames_df[relevant_columns].copy()
        
        # Rename columns for clarity
        cleaned_df = cleaned_df.rename(columns={
            'id': 'climate_id',
            'name': 'station_name',
            'feature.id': 'feature_id'
        })
        
        return cleaned_df
        
    except Exception as e:
        logger.error(f"Error loading geonames data: {e}")
        raise

def load_weather_data(raw_data_dir=RAW_DATA_DIR):
    """
    Load and process the weather data from raw CSV files
    
    Args:
        raw_data_dir (str): Directory containing raw data files
        
    Returns:
        pd.DataFrame: Processed weather data
    """
    logger.info(f"Loading weather data from raw files in {raw_data_dir}")
    
    # Find all CSV files in the raw data directory
    csv_files = glob.glob(f"{raw_data_dir}/station_*.csv")
    
    if not csv_files:
        logger.error(f"No raw data files found in {raw_data_dir}")
        raise FileNotFoundError(f"No raw data files found in {raw_data_dir}")
    
    all_data = []
    
    for file in csv_files:
        try:
            # Extract station ID and year from filename
            filename = os.path.basename(file)
            parts = filename.replace(".csv", "").split("_")
            station_id = parts[1]
            year = parts[2]
            
            logger.info(f"Processing {filename}")
            
            # Load data
            df = pd.read_csv(file)
            
            # Add station ID and year as columns
            df['station_id'] = station_id
            df['data_year'] = year
            
            all_data.append(df)
            
        except Exception as e:
            logger.error(f"Error processing file {file}: {e}")
    
    if not all_data:
        logger.error("No valid data files were processed")
        raise ValueError("No valid data files were processed")
    
    # Combine all data
    combined_df = pd.concat(all_data, ignore_index=True)
    
    return combined_df

def transform_weather_data(weather_df):
    """
    Transform the weather data into monthly aggregations
    
    Args:
        weather_df (pd.DataFrame): Raw weather data
        
    Returns:
        pd.DataFrame: Transformed data aggregated by month
    """
    logger.info("Transforming weather data")
    
    try:
        # Check for the date/time column with different possible names
        datetime_column = None
        if 'Date/Time (LST)' in weather_df.columns:
            datetime_column = 'Date/Time (LST)'
        elif 'Date/Time' in weather_df.columns:
            datetime_column = 'Date/Time'
        
        if not datetime_column:
            logger.error("Date/Time column not found in weather data")
            available_cols = ', '.join(weather_df.columns)
            logger.info(f"Available columns: {available_cols}")
            raise ValueError("Date/Time column not found in weather data")
        
        # Convert date string to datetime
        weather_df['datetime'] = pd.to_datetime(weather_df[datetime_column])
        
        # Extract month and year for aggregation
        weather_df['year'] = weather_df['datetime'].dt.year
        weather_df['month'] = weather_df['datetime'].dt.month
        weather_df['date_month'] = weather_df['datetime'].dt.strftime('%Y-%m')
        
        # Check if temperature column exists
        temp_column = None
        if 'Temp (°C)' in weather_df.columns:
            temp_column = 'Temp (°C)'
        elif 'Mean Temp (°C)' in weather_df.columns:
            temp_column = 'Mean Temp (°C)'
        
        if not temp_column:
            logger.error("Temperature column not found in weather data")
            available_cols = ', '.join(weather_df.columns)
            logger.info(f"Available columns: {available_cols}")
            raise ValueError("Temperature column not found in weather data")
        
        # Filter out rows with missing temperature values
        weather_df = weather_df.dropna(subset=[temp_column])
        
        # Group by station and month
        grouped = weather_df.groupby(['station_id', 'year', 'month']).agg({
            temp_column: ['mean', 'min', 'max'],
            'date_month': 'first'
        })
        
        # Flatten the multi-index columns
        grouped.columns = ['_'.join(col).strip() for col in grouped.columns.values]
        
        # Reset index to get regular columns
        result_df = grouped.reset_index()
        
        # Rename columns for clarity
        result_df = result_df.rename(columns={
            f'{temp_column}_mean': 'temperature_celsius_avg',
            f'{temp_column}_min': 'temperature_celsius_min',
            f'{temp_column}_max': 'temperature_celsius_max',
            'date_month_first': 'date_month'
        })
        
        return result_df
        
    except Exception as e:
        logger.error(f"Error transforming weather data: {e}")
        raise

def calculate_yoy_delta(df):
    """
    Calculate year-on-year temperature delta
    
    Args:
        df (pd.DataFrame): Monthly aggregated data
        
    Returns:
        pd.DataFrame: Data with YoY temperature delta added
    """
    logger.info("Calculating year-on-year temperature delta")
    
    # Create a copy to avoid modifying the original
    result_df = df.copy()
    
    # Sort by station, month, and year
    result_df = result_df.sort_values(['station_id', 'month', 'year'])
    
    # Calculate YoY delta by comparing with previous year's same month
    result_df['temperature_celsius_yoy_avg'] = result_df.groupby(['station_id', 'month'])['temperature_celsius_avg'].diff()
    
    return result_df

def join_weather_and_geonames(weather_df, geonames_df):
    """
    Join the weather and geonames data
    
    Args:
        weather_df (pd.DataFrame): Transformed weather data
        geonames_df (pd.DataFrame): Geonames data
        
    Returns:
        pd.DataFrame: Combined dataset
    """
    logger.info("Joining weather and geonames data")
    
    try:
        # Since the station_id in weather data might not directly match climate_id in geonames,
        # we would typically need a mapping table. For this exercise, we'll create a simple mapping
        # based on the stations we know about.
        
        # Create a simple join key for demonstration
        # In a real scenario, you would have a proper mapping between station_id and climate_id
        weather_df['join_key'] = 1
        geonames_df['join_key'] = 1
        
        # Perform a cross join (Cartesian product)
        # Note: In a real scenario, you would use a proper mapping/join condition
        combined_df = pd.merge(weather_df, geonames_df, on='join_key')
        
        # Drop the temporary join key
        combined_df = combined_df.drop('join_key', axis=1)
        
        # Select and order the final columns
        final_columns = [
            'station_name', 'climate_id', 'latitude', 'longitude', 'date_month',
            'feature_id', 'map', 'temperature_celsius_avg', 'temperature_celsius_min',
            'temperature_celsius_max', 'temperature_celsius_yoy_avg'
        ]
        
        # Ensure all required columns exist
        for col in final_columns:
            if col not in combined_df.columns:
                logger.error(f"Required column '{col}' not found in combined data")
                available_cols = ', '.join(combined_df.columns)
                logger.info(f"Available columns: {available_cols}")
                raise ValueError(f"Required column '{col}' not found in combined data")
        
        final_df = combined_df[final_columns].copy()
        
        return final_df
        
    except Exception as e:
        logger.error(f"Error joining weather and geonames data: {e}")
        raise

def run_transformation_pipeline(raw_data_dir=RAW_DATA_DIR, output_dir=OUTPUT_DIR, geonames_file=GEONAMES_FILE, output_file="weather_station_monthly.csv"):
    """
    Run the complete transformation pipeline
    
    Args:
        raw_data_dir (str): Directory containing raw data files
        output_dir (str): Directory to save output files
        geonames_file (str): Path to geonames CSV file
        output_file (str): Name of the output file
    
    Returns:
        str: Path to the output file
    """
    # Ensure output directory exists
    ensure_dir(output_dir)
    
    try:
        # Load geonames data
        geonames_df = load_geonames_data(geonames_file)
        logger.info(f"Loaded geonames data with {len(geonames_df)} records")
        
        # Load weather data
        weather_df = load_weather_data(raw_data_dir)
        logger.info(f"Loaded weather data with {len(weather_df)} records")
        
        # Transform weather data (monthly aggregation)
        monthly_df = transform_weather_data(weather_df)
        logger.info(f"Transformed to {len(monthly_df)} monthly records")
        
        # Calculate YoY delta
        monthly_df = calculate_yoy_delta(monthly_df)
        
        # Join with geonames data
        final_df = join_weather_and_geonames(monthly_df, geonames_df)
        logger.info(f"Final dataset has {len(final_df)} records")
        
        # Save final dataset
        output_path = os.path.join(output_dir, output_file)
        final_df.to_csv(output_path, index=False)
        logger.info(f"Saved final dataset to {output_path}")
        
        return output_path
        
    except Exception as e:
        logger.error(f"Error in data transformation: {e}")
        raise

def run_analysis(input_path, db_path=DEFAULT_DB_PATH):
    """
    Run the analysis on the transformed data
    
    Args:
        input_path (str): Path to the input CSV file
        db_path (str): Path to the SQLite database file
    
    Returns:
        dict: Dictionary of query results
    """
    if not os.path.exists(input_path):
        logger.error(f"Input file not found: {input_path}")
        raise FileNotFoundError(f"Input file not found: {input_path}")
    
    try:
        # Load data to SQLite
        conn = load_data_to_sqlite(input_path, db_path)
        
        # Run SQL queries
        results = run_sql_queries(conn)
        
        # Close connection
        conn.close()
        
        return results
        
    except Exception as e:
        logger.error(f"Error in data analysis: {e}")
        raise

def load_data_to_sqlite(input_path, db_path):
    """
    Load the CSV data into SQLite database
    
    Args:
        input_path (str): Path to the input CSV file
        db_path (str): Path to the SQLite database file
    
    Returns:
        sqlite3.Connection: Connection to the SQLite database
    """
    logger.info(f"Loading data from {input_path} into SQLite database")
    
    # Read CSV data
    df = pd.read_csv(input_path)
    
    # Create a SQLite connection
    conn = sqlite3.connect(db_path)
    
    # Load data into SQLite
    df.to_sql('weather_data', conn, if_exists='replace', index=False)
    
    logger.info(f"Loaded {len(df)} records into SQLite database")
    
    return conn

def run_sql_queries(conn):
    """
    Run various SQL queries on the data
    
    Args:
        conn (sqlite3.Connection): Connection to the SQLite database
        
    Returns:
        dict: Dictionary of query results
    """
    logger.info("Running SQL queries")
    
    query_results = {}
    
    # SQL query 1: Average temperature by station and year
    query1 = """
    SELECT 
        station_name,
        substr(date_month, 1, 4) as year,
        round(avg(temperature_celsius_avg), 2) as avg_temperature
    FROM 
        weather_data
    GROUP BY 
        station_name, year
    ORDER BY 
        station_name, year
    """
    
    logger.info("Running Query 1: Average temperature by station and year")
    query_results['avg_temp_by_station_year'] = pd.read_sql_query(query1, conn)
    
    # SQL query 2: Monthly temperature variations
    query2 = """
    SELECT 
        station_name,
        substr(date_month, 6, 2) as month,
        round(avg(temperature_celsius_max - temperature_celsius_min), 2) as avg_daily_variation
    FROM 
        weather_data
    GROUP BY 
        station_name, month
    ORDER BY 
        station_name, month
    """
    
    logger.info("Running Query 2: Monthly temperature variations")
    query_results['monthly_temp_variations'] = pd.read_sql_query(query2, conn)
    
    # SQL query 3: Year-over-year temperature change
    query3 = """
    SELECT 
        station_name,
        substr(date_month, 6, 2) as month,
        round(avg(temperature_celsius_yoy_avg), 2) as avg_yoy_change
    FROM 
        weather_data
    WHERE 
        temperature_celsius_yoy_avg IS NOT NULL
    GROUP BY 
        station_name, month
    ORDER BY 
        station_name, month
    """
    
    logger.info("Running Query 3: Year-over-year temperature change")
    query_results['yoy_temp_change'] = pd.read_sql_query(query3, conn)
    
    # SQL query 4: Most extreme temperature months
    query4 = """
    SELECT 
        station_name,
        date_month,
        temperature_celsius_max as max_temperature
    FROM 
        weather_data
    ORDER BY 
        temperature_celsius_max DESC
    LIMIT 10
    """
    
    logger.info("Running Query 4: Most extreme temperature months (highest max)")
    query_results['extreme_high_temps'] = pd.read_sql_query(query4, conn)
    
    # SQL query 5: Coldest temperature months
    query5 = """
    SELECT 
        station_name,
        date_month,
        temperature_celsius_min as min_temperature
    FROM 
        weather_data
    ORDER BY 
        temperature_celsius_min ASC
    LIMIT 10
    """
    
    logger.info("Running Query 5: Most extreme temperature months (lowest min)")
    query_results['extreme_low_temps'] = pd.read_sql_query(query5, conn)
    
    return query_results

def save_analysis_results(results, output_dir=OUTPUT_DIR, output_file="analysis_results.json"):
    """
    Save analysis results to a JSON file
    
    Args:
        results (dict or str): Dictionary of query results or stringified results from XCom
        output_dir (str): Directory to save output file
        output_file (str): Name of the output file
    
    Returns:
        str: Path to the output file
    """
    # FAILSAFE: Direct check to prevent the error on line 653
    if isinstance(results, str):
        logger.warning("FAILSAFE: Results is a string, converting to simple dict")
        output_path = os.path.join(output_dir, output_file)
        with open(output_path, 'w') as f:
            json.dump({"raw_data": results}, f, indent=2)
        logger.info(f"Saved raw string data to {output_path}")
        return output_path
    
    # Ensure output directory exists
    ensure_dir(output_dir)
    
    # Log the input type for debugging
    logger.info(f"save_analysis_results received data of type: {type(results)}")
    
    results_dict = {}
    
    try:
        # Handle different input types
        if isinstance(results, dict):
            # Direct dictionary input (expected in normal flow)
            logger.info("Processing results as dictionary")
            results_dict = {name: df.to_dict(orient='records') if hasattr(df, 'to_dict') else df 
                           for name, df in results.items()}
        
        elif isinstance(results, str):
            # String input from XCom
            logger.info("Processing results as string")
            try:
                # Try to parse as JSON
                json_data = json.loads(results)
                if isinstance(json_data, dict):
                    logger.info("Successfully parsed string as JSON dictionary")
                    results_dict = json_data
                else:
                    logger.warning(f"Parsed JSON is not a dictionary: {type(json_data)}")
                    results_dict = {"raw_data": results}
            except (json.JSONDecodeError, ValueError) as e:
                logger.warning(f"Could not parse results as JSON: {e}")
                results_dict = {"raw_data": results}
        
        elif results is None:
            # Handle None case
            logger.warning("Received None as results")
            results_dict = {"error": "No results data available"}
        
        else:
            # Handle any other type
            logger.warning(f"Unexpected results type: {type(results)}")
            results_dict = {"raw_data": str(results)}
    
    except Exception as e:
        logger.error(f"Error processing results: {e}")
        results_dict = {"error": str(e)}
    
    # Save to JSON
    output_path = os.path.join(output_dir, output_file)
    try:
        with open(output_path, 'w') as f:
            json.dump(results_dict, f, indent=2)
        logger.info(f"Saved analysis results to {output_path}")
    except Exception as e:
        logger.error(f"Error saving results to file: {e}")
        raise
    
    return output_path 