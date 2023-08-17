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
def connect_to_db(host, dbname, user, password):
    try:
        conn = connect(f"host={host} dbname={dbname} user={user} password={password}")
        logging.info("Connected to db successfully.")
        conn.set_session(autocommit=True)
        return conn
    except Exception as e:
        logging.error(f"Could not connect to database: {e}", exc_info=True)
    
def create_cursor(conn):
    try:
        cur = conn.cursor()
        logging.info("Cursor created.")
        return cur
    except Error as e: 
        logging.error(f"Cursor could not be created: {e}")

def create_database(database_name, cursor, conn):
    cursor.execute(sql.SQL("SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s"), [database_name])
    exists = cursor.fetchone()
    try:
        if exists is None:
            # Create the database
            cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database_name)))
            logging.info(f"Database {database_name} created successfully.")

        else:
            logging.info(f"Database {database_name} already exists.")
    except Error as e: 
        logging.error(f"There was an issue creating the database: {e}")
    except Exception as e:
        logging.error(f"There was an issue creating the database: {e}")

def create_table(table_name, cursor, column_dict: dict):
    try:
        columns = ', '.join([f'{col_name} {data_type}' for col_name, data_type in column_dict.items()])
        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns});"
        cursor.execute(create_table_query)
        logging.info(f'Table {table_name} was successfully created.')
    except Error as e:
        logging.error(f'There was an issue creating {table_name} table: {e}.')
    except Exception as e:
        logging.error(f'There was an issue creating {table_name} table: {e}.')

def insert_into_table(table_name, column_names: list, values: list, cursor):
    try:
        columns = ', '.join(column_names)
        place_holders = ', '.join(['%s'] * len(values))
        query = f"INSERT INTO {table_name} ({columns}) VALUES ({place_holders})"
        cursor.execute(query, values) # Passing values as the list itself
        logging.info(f"Successfully added {values} to {table_name}.")
    except Error as e: 
        logging.error(f"There was an issue adding the records to {table_name}: {e}")
    except Exception as e:
        logging.error(f"There was an issue adding the records to {table_name}: {e}")

def main():
    # initalize variables for default connection 
    host = '127.0.0.1'
    dbname = 'postgres'
    user = 'postgres'
    password = 'password'

    # connect to default db
    conn = connect_to_db(host='127.0.0.1', dbname='postgres', user='postgres', password='password')
    cur =  create_cursor(conn)
    create_database('data_modeling', cur, conn)

    # connect to new db after verifying exists or created
    conn = connect_to_db(host=host, dbname='data_modeling', user=user, 
                         password=password)
    cur =  create_cursor(conn)

    # create students table
    column_dict = {'student_id':'int', 'name':'varchar', 'age':'int', 
                   'gender':'varchar', 'subject':'varchar', 'marks':'int'}
    create_table(table_name='students', cursor=cur, column_dict=column_dict)

    #insert data 
    values = [1, "Raj", 23, "Male", "Python", 85]
    columns = [col_name for col_name in column_dict.keys()]
    insert_into_table(table_name='students', column_names=columns, values=values, 
                    cursor=cur)
    values = [2, "Priya", 22, "Female", "Python", 86]
    columns = [col_name for col_name in column_dict.keys()]
    insert_into_table(table_name='students', column_names=columns, values=values, 
                      cursor=cur)   
    cur.close()
    conn.close()
if __name__ == '__main__':
    main()