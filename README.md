# Weather Data ETL Pipeline

This project implements a data extraction, transformation, and loading (ETL) pipeline for weather data from the Canadian climate data API.

## Project Overview

The pipeline performs the following tasks:

1. **Data Extraction**:
   - Retrieves weather data from the climate.weather.gc.ca API for station IDs 26953 and 31688 for years 2023 and 2024
   - Performs basic data quality checks (nulls, outliers, missing days)
   - Stores the raw data as CSV files

2. **Data Transformation**:
   - Loads the geonames.csv file containing metadata about weather stations
   - Processes and cleans the raw weather data
   - Combines/joins the datasets
   - Aggregates data to station and month level
   - Calculates required measurements (avg, min, max temperatures, year-over-year changes)
   - Outputs the final dataset as CSV

3. **Data Analysis**:
   - Loads the transformed data into a SQLite database
   - Runs SQL queries for various insights and analyses
   - Displays the analysis results

## Project Structure

```
.
├── data_extraction.py     # Script for extracting data from the API
├── data_transformation.py # Script for transforming and joining data
├── data_analysis.py       # Script for analyzing data using SQL
├── test_pipeline.py       # Unit tests for the pipeline components
├── geonames.csv           # Dimension table with station metadata
├── raw_data/              # Directory for storing raw data files
├── output/                # Directory for storing the final output
├── run_pipeline.sh        # Shell script to run the complete pipeline
└── README.md              # Project documentation
```

## Requirements

- Python 3.8+
- pandas
- numpy
- requests
- sqlite3 (included in Python standard library)

## Installation

```bash
pip install -r requirements.txt
```

## Usage

### Option 1: Running the Complete Pipeline

Use the shell script to run the entire pipeline:

```bash
./run_pipeline.sh
```

Optional parameters:
- `-y, --year YEAR`: Specify a specific year (default: both 2023 and 2024)
- `-s, --station ID`: Specify a specific station ID (default: both 26953 and 31688)
- `-e, --extract-only`: Run only the extraction step
- `-t, --transform-only`: Run only the transformation step

### Option 2: Running Individual Steps

#### 1. Data Extraction

```bash
python data_extraction.py [--stations STATION_IDS] [--years YEARS] [--output-dir DIR]
```

For example, to extract data for a specific station and year:
```bash
python data_extraction.py --stations 26953 --years 2024 --output-dir my_data
```

#### 2. Data Transformation

```bash
python data_transformation.py [--raw-data-dir DIR] [--output-dir DIR] [--geonames-file FILE] [--output-file FILE]
```

#### 3. Data Analysis

```bash
python data_analysis.py [--input-dir DIR] [--input-file FILE] [--db-path PATH]
```

### Running Tests

The project includes unit tests to verify the functionality of key components. To run the tests:

```bash
python -m unittest test_pipeline.py
```

Or you can run the test script directly:

```bash
./test_pipeline.py
```

The tests cover:
- Data extraction functions (API fetching, data quality checks)
- Data transformation functions (aggregation, YoY calculations)
- Data analysis functions (SQLite loading, SQL queries)

## Output Schema

The final dataset includes the following columns:

- `station_name`: Name of the weather station
- `climate_id`: Unique ID for the station
- `latitude`: Station latitude
- `longitude`: Station longitude
- `date_month`: Year-month in YYYY-MM format
- `feature_id`: Feature ID from the geonames data
- `map`: Map reference from the geonames data
- `temperature_celsius_avg`: Average temperature for the month
- `temperature_celsius_min`: Minimum temperature for the month
- `temperature_celsius_max`: Maximum temperature for the month
- `temperature_celsius_yoy_avg`: Year-on-year average temperature delta

## Data Quality Checks

The pipeline performs the following data quality checks:

- Checks for null values in the data
- Identifies missing days in the time series
- Detects temperature outliers (values beyond 3 standard deviations)

## SQL Analysis

The data analysis script performs the following SQL queries:

1. **Average Temperature by Station and Year**: Calculates the average temperature for each station and year
2. **Monthly Temperature Variations**: Analyzes the average daily temperature variations by month
3. **Year-over-Year Temperature Change**: Examines the average YoY temperature changes by month
4. **Extreme High Temperatures**: Identifies the months with the highest maximum temperatures
5. **Extreme Low Temperatures**: Identifies the months with the lowest minimum temperatures

## Error Handling

The scripts include error handling to:

- Retry API requests if they fail
- Log errors and warnings
- Validate required columns and data formats
- Handle missing or invalid data

## Logging

All scripts use Python's logging module to provide information about the execution process, warnings, and errors. 