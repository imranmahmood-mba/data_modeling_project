import os
from psycopg2 import connect, Error
import logging
import logging.config
from psycopg2 import sql

# Get the absolute directory of the current script
abs_dir = os.path.dirname(os.path.abspath(__file__))

# Construct the absolute path to the logging configuration file
logging_config_path = os.path.join(abs_dir, '..', 'util', 'logging_to_file.conf')

logging.config.fileConfig(fname=logging_config_path)

# Get the custom Logger from Configuration File
logger = logging.getLogger(__name__)

class DataBase:
    def __init__(self, host, dbname, user, password):
        self.host = host
        self.dbname = 'postgres'  # Connect to a default database
        self.user = user
        self.password = password
        self.conn = None
        self.cur = None
        self.connect_to_db()
        self.create_database(dbname)
        self.conn.close()  # Close the connection to the default database
        self.dbname = dbname  # Update to the target database
        self.connect_to_db()  # Reconnect to the target database

    def connect_to_db(self):
        try:
            self.conn = connect(f"host={self.host} dbname={self.dbname} user={self.user} password={self.password}")
            logging.info("Connected to db successfully.")
            self.conn.set_session(autocommit=True)
            self.create_cursor()
        except Exception as e:
            logging.error(f"Could not connect to database: {e}", exc_info=True)
    
    def create_cursor(self):
        try:
            self.cur = self.conn.cursor()
            logging.info("Cursor created.")
            return self.cur
        except Error as e: 
            logging.error(f"Cursor could not be created: {e}")

    def create_database(self, database_name):
        self.cur.execute(sql.SQL("SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s"), [database_name])
        exists = self.cur.fetchone()
        try:
            if exists is None:
                # Create the database
                self.cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database_name)))
                logging.info(f"Database {database_name} created successfully.")

            else:
                logging.info(f"Database {database_name} already exists.")
        except Error as e: 
            logging.error(f"There was an issue creating the database: {e}")
        except Exception as e:
            logging.error(f"There was an issue creating the database: {e}")

    def create_table(self, table_name: str, column_dict: dict):
        try:
            columns = ', '.join([f'{col_name} {data_type}' for col_name, data_type in column_dict.items()])
            create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns});"
            self.cur.execute(create_table_query)
            logging.info(f'Table {table_name} was successfully created.')
        except Error as e:
            logging.error(f'There was an issue creating {table_name} table: {e}.')
        except Exception as e:
            logging.error(f'There was an issue creating {table_name} table: {e}.')

    def insert_into_table(self, table_name, column_names: list, values: list):
        try:
            columns = ', '.join(column_names)
            place_holders = ', '.join(['%s'] * len(values))
            query = f"INSERT INTO {table_name} ({columns}) VALUES ({place_holders})"
            self.cur.execute(query, values) # Passing values as the list itself
            logging.info(f"Successfully added {values} to {table_name}.")
        except Error as e: 
            logging.error(f"There was an issue adding the records to {table_name}: {e}")
        except Exception as e:
            logging.error(f"There was an issue adding the records to {table_name}: {e}")