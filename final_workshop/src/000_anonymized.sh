#!/bin/bash

# This script anonymizes the data from the input json file and writes the result to the output csv file.

echo "Anonymizing data..."
python 000_anonymized.py -i "..\data\raw\raw_data_sample.json" -o "..\data\processed\anonymized_data_sample.csv"
echo "Data anonymized Done"