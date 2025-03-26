# Weather Data Pipeline with Apache Airflow

This directory contains the Airflow implementation of the weather data ETL pipeline.

## Overview

The pipeline:
1. Extracts weather data from multiple stations for multiple years
2. Performs data quality checks
3. Transforms the data into monthly aggregations
4. Joins with geonames reference data
5. Performs SQL-based analysis
6. Saves the results

## Files

- `weather_etl_dag.py`: The main Airflow DAG definition
- `weather_utils.py`: Utility functions for the pipeline tasks

## Setup Instructions

### 1. Install Airflow

First, install Apache Airflow:

```bash
pip install apache-airflow
```

For a production setup, follow the [official installation guide](https://airflow.apache.org/docs/apache-airflow/stable/start/local.html).

### 2. Initialize Airflow

```bash
# Set the Airflow home directory
export AIRFLOW_HOME=~/airflow

# Initialize the database
airflow db init

# Create a user (for the web interface)
airflow users create \
    --username admin \
    --firstname <first_name> \
    --lastname <last_name> \
    --role Admin \
    --email <email>
```

### 3. Copy DAG Files

Copy the DAG files to your Airflow DAGs directory:

```bash
cp weather_etl_dag.py weather_utils.py $AIRFLOW_HOME/dags/
```

### 4. Start Airflow Services

```bash
# Start the web server
airflow webserver --port 8080

# In another terminal, start the scheduler
airflow scheduler
```

### 5. Access the Web Interface

Open your browser and navigate to `http://localhost:8080` to access the Airflow web interface.

## Using the Pipeline

### Triggering the DAG

1. Login to the Airflow web interface
2. Navigate to the DAGs page
3. Find the "weather_data_pipeline" DAG
4. Click the "Trigger DAG" button to manually run the pipeline

### Customizing the Pipeline

To customize the stations or years:

1. Edit the `DEFAULT_STATIONS` and `DEFAULT_YEARS` variables in `weather_etl_dag.py`
2. Restart the Airflow scheduler to apply changes

## Pipeline Architecture

The pipeline uses a dynamic task generation approach to create extraction tasks for each station and year combination.

### Task Flow

1. `start_pipeline`: Marks the beginning of the pipeline
2. `extract_station_X_Y`: Extract data for station X and year Y (multiple tasks generated dynamically)
3. `check_extractions_completed`: Waits for all extraction tasks to complete
4. `check_data_quality`: Decides whether to proceed with transformation or skip
5. `run_transformation`: Transforms the extracted data
6. `run_analysis`: Analyzes the transformed data
7. `save_analysis_results`: Saves the analysis results
8. `end_pipeline`: Marks the end of the pipeline

### Branching Logic

The pipeline includes branching logic based on data quality:
- If extraction tasks succeed and data quality is acceptable, transformation proceeds
- If any issues occur, transformation can be skipped but analysis will still run on any existing data

### Error Handling

The pipeline includes:
- Retry mechanisms for extraction tasks
- Timeout and poke interval settings for sensors
- Trigger rules to handle partial successes

## Monitoring

The Airflow web interface provides:
- Task-level monitoring and logging
- Success/failure tracking
- Execution time metrics
- XCom inspection for task outputs

## Advanced Features

This implementation includes:
- Dynamic task generation
- Conditional execution paths
- Data quality checks and branching
- XCom for inter-task communication
- Parameterized task definitions 