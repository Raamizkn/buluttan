from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.sensors.python import PythonSensor
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import os
import sys
import json
import logging

# Add parent directory to path for imports
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
from weather_utils import (
    fetch_weather_data, save_quality_report, run_transformation_pipeline,
    run_analysis, save_analysis_results, ensure_dir
)

# Constants
RAW_DATA_DIR = "raw_data"
OUTPUT_DIR = "output"
GEONAMES_FILE = "geonames.csv"
DEFAULT_STATIONS = [26953, 31688]
DEFAULT_YEARS = [2023, 2024]

# Default arguments for the DAG
default_args = {
    'owner': 'weather_team',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(hours=1)
}

# Create the DAG
dag = DAG(
    'weather_data_pipeline',
    default_args=default_args,
    description='Weather data ETL pipeline',
    schedule_interval='@daily',
    catchup=False,
    tags=['weather', 'etl']
)

# Function to fetch data for a specific station and year
def fetch_station_data(station_id, year, **kwargs):
    """Fetch data for a specific station and year"""
    ensure_dir(RAW_DATA_DIR)
    output_file, quality_results = fetch_weather_data(
        station_id=station_id,
        year=year,
        month=1,
        day=1,
        output_dir=RAW_DATA_DIR
    )
    
    # Store quality results in XCom
    task_instance = kwargs['ti']
    task_instance.xcom_push(
        key=f'quality_results_{station_id}_{year}',
        value=quality_results
    )
    
    return output_file

# Function to check if all extraction tasks succeeded
def check_extractions(**kwargs):
    """Check if all extraction tasks completed successfully"""
    task_instance = kwargs['ti']
    all_quality_results = {}
    
    for station_id in DEFAULT_STATIONS:
        for year in DEFAULT_YEARS:
            key = f'quality_results_{station_id}_{year}'
            quality_result = task_instance.xcom_pull(key=key)
            if quality_result:
                all_quality_results[f'station_{station_id}_{year}'] = quality_result
    
    # Save quality report
    if all_quality_results:
        save_quality_report(all_quality_results, RAW_DATA_DIR)
        return True
    
    return False

# Function to decide whether to proceed with transformation
def check_data_quality(**kwargs):
    """Check data quality and decide whether to proceed"""
    task_instance = kwargs['ti']
    all_quality_results = {}
    
    for station_id in DEFAULT_STATIONS:
        for year in DEFAULT_YEARS:
            key = f'quality_results_{station_id}_{year}'
            quality_result = task_instance.xcom_pull(key=key)
            if quality_result:
                all_quality_results[f'station_{station_id}_{year}'] = quality_result
    
    # Check if we have any data
    if not all_quality_results:
        return 'skip_transformation'
    
    # In a production environment, we might implement more sophisticated
    # quality checks here to decide whether to proceed or not
    return 'run_transformation'

# Add this function at the top level, below the existing imports
def get_analysis_results_and_save(**context):
    """
    Retrieve analysis results from XCom and save them
    
    Args:
        context: Airflow context
        
    Returns:
        str: Path to saved file
    """
    logger = logging.getLogger(__name__)
    
    ti = context['ti']
    logger.info("Retrieving analysis results from XCom")
    
    try:
        results = ti.xcom_pull(task_ids='run_analysis')
        logger.info(f"XCom pull returned data of type: {type(results)}")
        
        if results is None:
            logger.warning("No results found in XCom, creating empty result")
            results = {"error": "No results found in XCom"}
        
        logger.info("Calling save_analysis_results function")
        return save_analysis_results(results, OUTPUT_DIR, "analysis_results.json")
    
    except Exception as e:
        logger.error(f"Error in get_analysis_results_and_save: {e}")
        # Create a fallback output
        ensure_dir(OUTPUT_DIR)
        output_path = os.path.join(OUTPUT_DIR, "analysis_results_fallback.json")
        with open(output_path, 'w') as f:
            json.dump({"error": str(e)}, f)
        return output_path

# Add this after get_analysis_results_and_save
def run_analysis_wrapper(**context):
    """
    Run analysis and ensure proper serialization for XCom
    
    Args:
        context: Airflow context
        
    Returns:
        dict: Analysis results in a safe format for XCom
    """
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("Running analysis")
        results = run_analysis(
            input_path=f"{OUTPUT_DIR}/weather_station_monthly.csv",
            db_path="weather_data.db"
        )
        
        # Convert pandas DataFrames to dicts before XCom serialization
        logger.info("Converting analysis results for XCom")
        serializable_results = {}
        
        for key, df in results.items():
            if hasattr(df, 'to_dict'):
                serializable_results[key] = df.to_dict(orient='records')
            else:
                serializable_results[key] = df
        
        return serializable_results
    
    except Exception as e:
        logger.error(f"Error in run_analysis_wrapper: {e}")
        return {"error": str(e)}

# Create a start task
start = DummyOperator(
    task_id='start_pipeline',
    dag=dag
)

# Create extraction tasks dynamically
extraction_tasks = []
for station_id in DEFAULT_STATIONS:
    for year in DEFAULT_YEARS:
        task_id = f'extract_station_{station_id}_{year}'
        task = PythonOperator(
            task_id=task_id,
            python_callable=fetch_station_data,
            op_kwargs={'station_id': station_id, 'year': year},
            dag=dag
        )
        extraction_tasks.append(task)

# Data quality check sensor
quality_sensor = PythonSensor(
    task_id='check_extractions_completed',
    python_callable=check_extractions,
    timeout=600,  # 10 minutes timeout
    mode='poke',
    poke_interval=30,  # Check every 30 seconds
    dag=dag
)

# Branching to decide whether to transform or skip
quality_branch = BranchPythonOperator(
    task_id='check_data_quality',
    python_callable=check_data_quality,
    dag=dag
)

# Transformation task
transform_task = PythonOperator(
    task_id='run_transformation',
    python_callable=run_transformation_pipeline,
    op_kwargs={
        'raw_data_dir': RAW_DATA_DIR,
        'output_dir': OUTPUT_DIR,
        'geonames_file': GEONAMES_FILE,
        'output_file': 'weather_station_monthly.csv'
    },
    dag=dag
)

# Skip transformation task
skip_transform = DummyOperator(
    task_id='skip_transformation',
    dag=dag
)

# Analysis task
analysis_task = PythonOperator(
    task_id='run_analysis',
    python_callable=run_analysis_wrapper,
    provide_context=True,
    trigger_rule=TriggerRule.ONE_SUCCESS,
    dag=dag
)

# Save analysis results task
save_results_task = PythonOperator(
    task_id='save_analysis_results',
    python_callable=get_analysis_results_and_save,
    provide_context=True,
    dag=dag
)

# End task
end = DummyOperator(
    task_id='end_pipeline',
    trigger_rule=TriggerRule.ONE_SUCCESS,
    dag=dag
)

# Set up task dependencies
start >> extraction_tasks >> quality_sensor >> quality_branch
quality_branch >> transform_task >> analysis_task
quality_branch >> skip_transform >> analysis_task
analysis_task >> save_results_task >> end 