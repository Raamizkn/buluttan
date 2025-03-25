#!/bin/bash

# Data ETL Pipeline Runner
# This script runs the complete data pipeline: extraction and transformation

# Display usage information
usage() {
  echo "Usage: $0 [OPTIONS]"
  echo "Run the complete weather data ETL pipeline"
  echo
  echo "Options:"
  echo "  -y, --year YEAR      Specify a year (default: both 2023 and 2024)"
  echo "  -s, --station ID     Specify a station ID (default: both 26953 and 31688)"
  echo "  -e, --extract-only   Run only the extraction step"
  echo "  -t, --transform-only Run only the transformation step"
  echo "  -h, --help           Display this help message"
  exit 1
}

# Default values
EXTRACT=true
TRANSFORM=true

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case "$1" in
    -y|--year)
      if [[ -n "$2" && "$2" =~ ^[0-9]{4}$ ]]; then
        export WEATHER_YEAR="$2"
        shift 2
      else
        echo "Error: Year must be a 4-digit number"
        usage
      fi
      ;;
    -s|--station)
      if [[ -n "$2" && "$2" =~ ^[0-9]+$ ]]; then
        export WEATHER_STATION="$2"
        shift 2
      else
        echo "Error: Station must be a number"
        usage
      fi
      ;;
    -e|--extract-only)
      EXTRACT=true
      TRANSFORM=false
      shift
      ;;
    -t|--transform-only)
      EXTRACT=false
      TRANSFORM=true
      shift
      ;;
    -h|--help)
      usage
      ;;
    *)
      echo "Unknown option: $1"
      usage
      ;;
  esac
done

# Check if Python is installed
if ! command -v python3 &>/dev/null; then
  echo "Error: Python 3 is required but not installed"
  exit 1
fi

# Check if required packages are installed
echo "Checking dependencies..."
python3 -c "import pandas, numpy, requests" 2>/dev/null
if [ $? -ne 0 ]; then
  echo "Installing dependencies..."
  pip install -r requirements.txt
fi

# Run extraction step
if [ "$EXTRACT" = true ]; then
  echo "Starting data extraction..."
  python3 data_extraction.py
  if [ $? -ne 0 ]; then
    echo "Error: Data extraction failed"
    exit 1
  fi
  echo "Data extraction completed successfully"
fi

# Run transformation step
if [ "$TRANSFORM" = true ]; then
  echo "Starting data transformation..."
  python3 data_transformation.py
  if [ $? -ne 0 ]; then
    echo "Error: Data transformation failed"
    exit 1
  fi
  echo "Data transformation completed successfully"
fi

echo "Pipeline executed successfully" 