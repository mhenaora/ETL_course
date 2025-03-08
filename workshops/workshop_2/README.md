# Workshop 2

## Guide 
Follow guide reference at ```docs/transformations_workshop.pdf```

## Installation

To install and run the script, follow these steps:

```bash
# Clone Repository
git clone https://github.com/mhenaora/ETL_course.git
cd workshops/workshop_2

# Create venv 
python3 -m venv env
source env/bin/activate # source env/Scripts/activate 

# Install the dependencies
pip3 install -r requirements.txt
```

## Usage
Before executing make sure to upload ```MLA_100k.jsonlines``` and place it in ```data/MLA_100k.jsonlines```
Execute ```workshop_002_eda.ipynb``` notebook for results
Output CSV file is placed ```data/MLA_100k_clean_data.csv```
HTML Report ```workshop_002_eda.html```