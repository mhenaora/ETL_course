#!/bin/bash

# This script executes ETL process.

echo "Start ETL..."
# python 000_etl.py -i "..\data\raw\sample_raw_data_2025_02.csv"
python 000_etl.py -i "..\data\raw\sample_raw_data_2025_03.csv"
echo "Batch Transformed and saved in DB"