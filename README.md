# Netflix Database Project

This repository contains code to create and populate a database with Netflix data. It creates various tables such as the fact table, show dimension table, actor dimension table, etc.

The intention behind this project was to work on dimensional modeling. I used a snowflake dimensional model because I wanted to create denormalized database. 

The database was created locally using a docker container for a PostgreSQL instance. 

## Requirements

- Python 3.7 or higher
- pandas
- numpy
- create_database module
- data_preprocessing module
- dimensional_model_table_functions module

## Installation

Clone the repository:

\```bash
git clone https://github.com/imranmahmood-mba/data_modeling_project.git
cd netflix-database
\```

Install the required packages:

\```bash
pip install pandas numpy
\```

## Usage

Update the connection details in `main.py` for your specific database configuration:

\```python
host = '127.0.0.1'
dbname = 'netflix'
user = 'postgres'
password = 'password'
\```

Run the `main.py` script:

\```bash
python main.py
\```

This will create the necessary tables and populate them with the Netflix data.

## Tables Created

- `netflix_facts`
- `show_dimension`
- `actor_dimension`
- `movie_actor_dimension`
- `genre_dimension`
- `movie_genre_dimension`
- `type_dimension`
- `director_dimension`
- `movie_director_dimension`
- `country_dimension`
- `movie_country_dimension`

## Acknowledgments

- Inspired by Netflix's vast catalog
- Data from Shivam Bansel's dataset on Kaggle(https://www.kaggle.com/datasets/shivamb/netflix-shows)
- Project inspired by Darshil Parmar(https://www.youtube.com/@DarshilParmar)
