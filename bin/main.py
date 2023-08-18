import create_database as cdb

def main():
    # initalize variables for default connection 
    host = '127.0.0.1'
    dbname = 'netflix'
    user = 'postgres'
    password = 'password'

    # connect to default db
    db = cdb.DataBase(host=host, dbname=dbname, user=user, password=password)
    
    # create fact table
    column_dict = {'show_id':'varchar', 'actor_id':'int', 'type_id':'int', 
                   'director_id':'int', 'country_id':'int', 'date_id':'int',
                    'year_id':'int', 'duration':'varchar', 'genre_id':'int'}
    db.create_table(table_name='netflix_facts', column_dict=column_dict)

    # create dimension tables
    show_dimension_column_dict = {'show_id':'varchar', 'description':'varchar',
                                  'title':'varchar', 'type':'varchar', 
                                  'rating':'varchar'}
    db.create_table(table_name='show_dimension', columns=show_dimension_column_dict)

    actor_dimension_column_dict = {'actor_id':'int', 'actor':'varchar'}
    db.create_table(table_name='actor_dimension', columns=actor_dimension_column_dict)

    year_dimension_column_dict = {'year_id':'int', 'year':'varchar'}
    db.create_table(table_name='year_dimension', columns=year_dimension_column_dict)

    date_dimension_column_dict = {'year_id':'int', 'year':'int'}
    db.create_table(table_name='date_dimension', columns=date_dimension_column_dict)

    genre_dimension_column_dict = {'genre_id':'int', 'genre':'varchar'}
    db.create_table(table_name='genre_dimension', columns=genre_dimension_column_dict)

    type_dimension_column_dict = {'type_id':'int', 'type':'varchar'}
    db.create_table(table_name='type_dimension', columns=type_dimension_column_dict)
    
    director_dimension_column_dict = {'director_id':'int', 'director':'varchar'}
    db.create_table(table_name='director_dimension', columns=director_dimension_column_dict)

    country_dimension_column_dict = {'country_id':'int', 'country':'varchar'}
    db.create_table(table_name='country_dimension', columns=country_dimension_column_dict)
    

    #insert data 
    values = [1, "Raj", 23, "Male", "Python", 85]
    columns = [col_name for col_name in column_dict.keys()]
    db.insert_into_table(table_name='students', column_names=columns, values=values)
    values = [2, "Priya", 22, "Female", "Python", 86]
    columns = [col_name for col_name in column_dict.keys()]
    db.insert_into_table(table_name='students', column_names=columns, values=values)   
    
    db.cur.close()
    db.conn.close()