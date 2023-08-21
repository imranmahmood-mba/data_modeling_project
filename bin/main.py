import create_database as cdb
import data_preprocessing as dp
import pandas as pd
import numpy as np 
import dimensional_model_table_functions as dmtf

def main():
    # initalize variables for default connection 
    host = '127.0.0.1'
    dbname = 'netflix'
    user = 'postgres'
    password = 'password'

    # connect to default db
    db = cdb.DataBase(host=host, dbname=dbname, user=user, password=password)
    netflix_data = dp.NetflixDataset('../data/netflix_titles.csv').get_dataset

    # create tables
    dm = dmtf.DimensionalModel(db=db, netflix_data=netflix_data)
    dm.actor_dimension_table()
    dm.fact_table()
    dm.country_bridge_table()
    dm.country_dimension_table()
    dm.director_dimension_table()
    dm.genre_bridge_table()
    dm.genre_dimension_table()
    dm.movie_actors_bridge_table()
    dm.movie_director_dimension_table()
    dm.show_dimension_table()
    dm.type_dimension_table()

    db.cur.close()
    db.conn.close()

if __name__ == '__main__':
    main()