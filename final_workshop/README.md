# Final Workshop

## Context

The project aims to analyze the performance of a facial validation API using an
ETL process. The API, built on a microservices architecture, includes face detection, accessory
detection, image quality assessment, and Presentation Attack Detection (PAD) to prevent identity
spoofing. Performance data is stored in a non-relational database. The primary objectives are to identify
execution time bottlenecks of each microservice, analyze acceptance rates per client, and improve user
experience by examining retry patterns. 

## Guide 
Follow guide reference at ```docs/ETL_FINAL_WORKSHOP_MANUEL_HENAO.pdf```

## Installation

To install and run the script, follow these steps:

```bash
# Clone Repository
git clone https://github.com/mhenaora/ETL_course.git
cd final_workshop/

# Create venv 
python3 -m venv env
source env/bin/activate # source env/Scripts/activate 

# Install the dependencies
pip3 install -r requirements.txt

# Change to work directory
cd src/
```

## Usage

```
Make sure to include your YAML file for crendentials inside ```/src``` folder following this format:

```yaml
database:
  user: "user" 
  password: "password" 
  host: "localhost"
  port: 5432
  name: "db"
```

## Extract Stage
Due to the sensitive data involved, the script 000_anonymized.py (along with 000_anonymized.sh) demonstrates how sensitive data is anonymized and transformed from raw semi-structured data into a tabular format represented in a CSV file with the features mentioned in the model card.

Next, execute 001_staging.ipynb to stage the raw data (after anonymization) in a relational database.