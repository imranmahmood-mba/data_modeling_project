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


class DBConnection:
    def connect(self, host: str, dbname: str, user: str, password: str):
        try:
            conn = connect(f"host={host} dbname={dbname} user={user} password={password}")
            conn.set_session(autocommit=True)
            return conn, conn.cursor()
        except Exception as e:
            logging.error(f"Could not connect to database: {e}", exc_info=True) 


class DataBase:
    def __init__(self, host: str, dbname: str, user: str, password: str, connection_class=DBConnection):
        self.host = host
        self.user = user
        self.password = password
        self.connection_class = connection_class
        self.connect_to_default_db()
        self.create_database(dbname)
        self.reconnect_to_db(dbname)

    def connect_to_default_db(self):
        self.conn, self.cur = self.connection_class().connect(self.host, 'postgres', self.user, self.password)

    def reconnect_to_db(self, dbname: str):
        self.conn.close()
        self.conn, self.cur = self.connection_class().connect(self.host, dbname, self.user, self.password)

    def create_database(self, database_name: str):
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

    def insert_into_table(self, table_name: str, column_names: list, values: list):
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