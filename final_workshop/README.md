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
# Create venv 
python3 -m venv env
source env/bin/activate # source env/Scripts/activate 

# Install the dependencies
pip3 install -r requirements.txt

# Change to work directory
cd src/
```

## Usage


Make sure to include your YAML file for crendentials as ```config.yaml``` inside ```/src``` folder following this format:

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

## Transform Stage
After executing Extract Stage, the notebook ```002_eda_transform.ipynb``` demonstrates the EDA and transformations involved for this process.

The EDA reports for the staging tables and transform tables are in the [links below](https://drive.google.com/drive/folders/1dpNyqd8E91HgEhjNcTt3pyzZc8IJ5UKj?usp=sharing).

* [EDA_etl_staging_table_final_workshop](https://drive.google.com/file/d/11b6uNR4ldYpJKMj46Wj-c2HritL2tf6U/view?usp=sharing)
* [EDA_etl_transform_table_final_workshop](https://drive.google.com/file/d/11YJEN0j4cMYquPAadG3F7P_URyMDv0kB/view?usp=drive_link)
* [EDA_etl_transform_no_retries_table_final_workshop](https://drive.google.com/file/d/11oSa3zus3QgcRKIPVXJuf4mJn2-embUY/view?usp=sharing)
