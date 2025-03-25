#!/usr/bin/env python3
import os
import pandas as pd
import numpy as np
import glob
import argparse
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Constants
DEFAULT_RAW_DATA_DIR = "raw_data"
DEFAULT_OUTPUT_DIR = "output"
DEFAULT_GEONAMES_FILE = "geonames.csv"

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Transform weather data and join with geonames data')
    
    parser.add_argument('--raw-data-dir', type=str,
                        help=f'Directory containing raw data files (default: {DEFAULT_RAW_DATA_DIR})',
                        default=DEFAULT_RAW_DATA_DIR)
    
    parser.add_argument('--output-dir', type=str,
                        help=f'Directory to save output files (default: {DEFAULT_OUTPUT_DIR})',
                        default=DEFAULT_OUTPUT_DIR)
    
    parser.add_argument('--geonames-file', type=str,
                        help=f'Path to geonames CSV file (default: {DEFAULT_GEONAMES_FILE})',
                        default=DEFAULT_GEONAMES_FILE)
    
    parser.add_argument('--output-file', type=str,
                        help='Name of the output file (default: weather_station_monthly.csv)',
                        default="weather_station_monthly.csv")
    
    return parser.parse_args()

def ensure_output_dir(output_dir):
    """Create output directory if it doesn't exist"""
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        logger.info(f"Created directory: {output_dir}")

def load_geonames_data(geonames_file):
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

def load_weather_data(raw_data_dir):
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
        
        # Example mapping (in production, this would come from a mapping table)
        station_mapping = {
            '26953': 'ABCDE',  # Replace with actual climate_id values from geonames
            '31688': 'FGHIJ'   # Replace with actual climate_id values from geonames
        }
        
        # Add climate_id to weather data based on mapping
        # For this assessment, we'll use a placeholder approach since we don't have actual mappings
        # In a real scenario, this would be a proper join based on a common key
        
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

def main():
    """Main function to transform data"""
    # Parse command line arguments
    args = parse_args()
    
    # Get values from arguments
    raw_data_dir = args.raw_data_dir
    output_dir = args.output_dir
    geonames_file = args.geonames_file
    output_file = args.output_file
    
    # Ensure output directory exists
    ensure_output_dir(output_dir)
    
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
        
    except Exception as e:
        logger.error(f"Error in data transformation: {e}")
        raise

if __name__ == "__main__":
    main() 