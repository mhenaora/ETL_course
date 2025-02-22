# Workshop 1

## Guide 
Follow guide reference at ```docs/extract_excercise.pdf```

## Installation

To install and run the script, follow these steps:

```bash
# Clone Repository
git clone https://github.com/mhenaora/ETL_course.git
cd workshops/workshop_1

# Create venv 
python3 -m venv env
source env/bin/activate # source env/Scripts/activate 

# Install the dependencies
pip3 install -r requirements.txt
```

## Usage

Make sure to include your YAML file for crendentials following this format:

```yaml
database:
  user: "user" 
  password: "password" 
  host: "localhost"
  port: 5432
  name: "db"
```

Execute ```workshop_001_extract.ipynb``` notebook for results