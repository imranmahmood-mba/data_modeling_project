import unittest
from unittest.mock import patch, Mock
import create_database as cdb
import psycopg2

class TestDBConnection(unittest.TestCase):
    @patch('create_database.connect')
    @patch('create_database.logging.info')
    @patch('create_database.logging.error')
    def test_connect_to_db_successful(self, mock_error_logging, mock_info_logging, mock_connect):
        # Arrange
        mock_conn = Mock()
        mock_conn.set_session = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
         
        host, dbname, user, password = 'localhost', 'test_db', 'user', 'password'
        
        # Act 
        db = cdb.DBConnection()
        conn, cursor = db.connect(host=host, dbname=dbname, user=user, password=password)
        
        # Assert
        mock_connect.assert_called_with(f"host={host} dbname={dbname} user={user} password={password}")
        mock_conn.set_session.assert_called_once_with(autocommit=True)
        self.assertEqual(conn, mock_conn)
        self.assertEqual(cursor, mock_cursor)

class TestTableInsert(unittest.TestCase):
    @patch('create_database.logging.info') # Mocking the info logging method
    @patch('create_database.logger.error') # Mocking the error logging method
    @patch('create_database.DBConnection')
    def test_insert_into_table(self, mock_connect, mock_error_logging, mock_info_logging):
        # Arrange 
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_connect.return_value.connect.return_value = (mock_conn, mock_cursor)
        host, dbname, user, password = 'localhost', 'test_db', 'user', 'password'
        db = cdb.DataBase(host, dbname, user, password, connection_class=mock_connect) # Instantiate the object
        
        columns = ['col1', 'col2', 'col3']
        joined_columns = ', '.join(columns)
        values = ['1', 'abc', '383daf']
        place_holders = ', '.join(['%s'] * len(values)) # Creating placeholders for the values in the query
        table_name = 'test'
        
        query = f"INSERT INTO {table_name} ({joined_columns}) VALUES ({place_holders})"
        expected_query = "INSERT INTO test (col1, col2, col3) VALUES (%s, %s, %s)"
        self.assertEqual(query, expected_query) # Asserting that the constructed query matches the expected query
        
        db.cur = mock_cursor
        db.insert_into_table(table_name=table_name, column_names=columns, values=values)

        # Asserting that the mock cursor's execute method was called once with the expected query and values
        mock_cursor.execute.assert_called_with(query, values)
        mock_info_logging.assert_called_with(f"Successfully added {values} to {table_name}.") # Asserting that the info logging method was called with the correct message


class TestCreateTable(unittest.TestCase):
    @patch('create_database.logging.info')
    @patch('create_database.logging.error')
    @patch('create_database.DBConnection')
    def test_create_table(self, mock_connect, mock_error_logging, mock_info_logging):
        host, dbname, user, password = 'localhost', 'test_db', 'user', 'password'
        mock_cursor = Mock()
        mock_conn = Mock()
        mock_connect.return_value.connect.return_value = (mock_conn, mock_cursor)
        db = cdb.DataBase(host, dbname, user, password, connection_class=mock_connect) # Instantiate the object

        column_dict = {'col1':'int', 'col2':'varchar', 'col3':'varchar'}
        columns = ', '.join([f'{col_name} {data_type}' for col_name, data_type in column_dict.items()])
        table_name = 'test'
        
        query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns});"
        expected_query = 'CREATE TABLE IF NOT EXISTS test (col1 int, col2 varchar, col3 varchar);'
        self.assertEqual(query, expected_query)
        
        db.create_table(table_name=table_name,column_dict=column_dict)

        db.cur.execute.assert_called_with(query)
        mock_info_logging.assert_called()