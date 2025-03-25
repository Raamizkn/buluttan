#!/usr/bin/env python3
import os
import pandas as pd
import sqlite3
import argparse
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Constants
DEFAULT_OUTPUT_DIR = "output"
DEFAULT_INPUT_FILE = "weather_station_monthly.csv"
DEFAULT_DB_PATH = "weather_data.db"

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Analyze weather data using SQL')
    
    parser.add_argument('--input-dir', type=str,
                        help=f'Directory containing input CSV file (default: {DEFAULT_OUTPUT_DIR})',
                        default=DEFAULT_OUTPUT_DIR)
    
    parser.add_argument('--input-file', type=str,
                        help=f'Name of the input CSV file (default: {DEFAULT_INPUT_FILE})',
                        default=DEFAULT_INPUT_FILE)
    
    parser.add_argument('--db-path', type=str,
                        help=f'Path to SQLite database file (default: {DEFAULT_DB_PATH})',
                        default=DEFAULT_DB_PATH)
    
    return parser.parse_args()

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
    
    # Create a cursor
    cursor = conn.cursor()
    
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

def display_results(results):
    """
    Display the results of SQL queries
    
    Args:
        results (dict): Dictionary of query results
    """
    logger.info("Displaying query results")
    
    for query_name, df in results.items():
        print(f"\n=== {query_name} ===")
        print(df)
        print()

def main():
    """Main function to analyze data"""
    # Parse command line arguments
    args = parse_args()
    
    # Get values from arguments
    input_dir = args.input_dir
    input_file = args.input_file
    db_path = args.db_path
    
    # Full path to input file
    input_path = os.path.join(input_dir, input_file)
    
    if not os.path.exists(input_path):
        logger.error(f"Input file not found: {input_path}")
        return
    
    try:
        # Load data into SQLite
        conn = load_data_to_sqlite(input_path, db_path)
        
        # Run SQL queries
        results = run_sql_queries(conn)
        
        # Display results
        display_results(results)
        
        # Close connection
        conn.close()
        
    except Exception as e:
        logger.error(f"Error in data analysis: {e}")
        raise

if __name__ == "__main__":
    main() 