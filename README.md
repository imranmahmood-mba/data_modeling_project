# Netflix Database Project

This repository contains code to create and populate a database with Netflix data. It creates various tables such as the fact table, show dimension table, actor dimension table, etc.

The intention behind this project was to work on dimensional modeling and more specifically, a snowflake schema.

The database was created locally using a docker container for a PostgreSQL instance. 

## `create_database.py` - Overview

This Python script is part of the "bin" folder in the Data Modeling Project. It contains classes and functions to manage database connections and operations using the `psycopg2` library.

### Main Components

1. **DBConnection Class**: Manages the connection to the PostgreSQL database.
2. **DataBase Class**: Handles various database operations, including:
   - Connecting to the default database.
   - Creating a new database.
   - Reconnecting to a specific database.
   - Creating tables.
   - Inserting records into tables.
   - Setting primary and foreign keys.

### Usage

To use this script, you'll need to have the `psycopg2` library installed and provide the necessary connection parameters such as host, database name, user, and password.

### Logging

The script includes logging configuration to log information, warnings, and errors. The logging configuration file path is constructed within the script.

---

## `data_preprocessing.py` - Overview

The `data_preprocessing.py` script is part of the "bin" folder in the Data Modeling Project. It's focused on handling and transforming the Netflix dataset.

### Main Components

1. **NetflixDataset Class**: Manages the Netflix dataset, including:
   - Reading the CSV file into a DataFrame.
   - Splitting comma-separated values in a column and creating a row for each value.
   - Adding an ID to a column.
   - Providing access to the dataset.

2. **Functions for Data Transformation**:
   - `encode_column`: Encodes unique values in a specified column.
   - `create_content_dimension`: Separates duration into seasons and minutes, creates a content_id column.
   - `split_and_assign_id`: Splits a names column into multiple rows and assigns IDs.

### Usage

This script utilizes the `pandas` library to manipulate and transform the dataset. It provides functionalities to preprocess the data, such as encoding, splitting, and creating new dimensions.

### Logging

This script includes logging configuration to log information, warnings, and errors.

---

## `db_unittests.py` - Overview

The `db_unittests.py` script contains unit tests for the database-related functionalities, using Python's `unittest` framework.

### Main Components

1. **TestDBConnection Class**: Tests the connection to the database.
   - `test_connect_to_db_successful`: Ensures that the connection to the database is successful and the correct parameters are used.

2. **TestTableInsert Class**: Tests the insertion of records into tables.
   - `test_insert_into_table`: Validates the query construction and execution for inserting records into a table.

3. **TestCreateTable Class**: Tests the creation of tables.
   - `test_create_table`: Validates the query construction and execution for creating a table.

### Usage

These tests use mocking to isolate the code under test and ensure that the database-related functionalities are working as expected. The `unittest.mock` library is used to replace parts of the system under test with mock objects and make assertions about how they have been used.

---

## `dimensional_model_table_functions.py` - Overview

The `dimensional_model_table_functions.py` script defines the structure and relationships of the tables in the database.

### Main Components

1. **DimensionalModel Class**: Defines the structure and functionalities for creating various tables, including:
   - Fact Table: Contains the main facts like show_id, type_id, date_added, and release_year.
   - Show Dimension Table: Contains details like show_id, description, title, type, rating, minutes, and seasons.
   - Actor Dimension Table: Contains actor_id and actor name.
   - Movie Actors Bridge Table: Links actors to movies.
   - Genre Dimension Table: Contains genre_id and genre name.
   - Genre Bridge Table: Links genres to movies.
   - Type Dimension Table: Contains type_id and type name.
   - Director Dimension Table: Contains director_id and director name.
   - Director Bridge Table: Links directors to movies.
   - Country Dimension Table: Contains country_id and country name.
   - Country Bridge Table: Links countries to movies.

### Usage

This script leverages the functionalities defined in `create_database.py` and `data_preprocessing.py` to create and populate the tables.

---

## `main.py` - Overview

The `main.py` script is the entry point for the Data Modeling Project. It brings together all the other scripts in the "bin" folder and executes them in a coordinated sequence.

### Main Components

- **Initialization**: Sets up the default connection variables for the PostgreSQL database.
- **Database Connection**: Utilizes the `create_database.py` script to connect to the Netflix database.
- **Data Preprocessing**: Uses the `data_preprocessing.py` script to read and preprocess the Netflix dataset.
- **Dimensional Model Creation**: Leverages the `dimensional_model_table_functions.py` script to create and populate the tables in the database.

### Usage

This script is the entry point for the entire project. By running this script, you initiate the process of connecting to the database, preprocessing the data, and creating the dimensional model.
