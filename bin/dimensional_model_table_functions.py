import create_database as cdb
import data_preprocessing as dp
import numpy as np 

class DimensionalModel:
    def __init__(self, db, netflix_data):
        self.db = db
        self.netflix_data = netflix_data
    # create fact table
    def fact_table(self):
        column_dict = {'show_id':'varchar', 'type_id':'int', 'date_added':'date',
                        'release_year':'int'}
        fact_table = self.netflix_data.copy()

        dp.encode_column(fact_table, 'type')
        fact_table.rename(columns={'type':'type_id'}, inplace=True)
        fact_table = fact_table[column_dict.keys()]
        self.db.create_table(table_name='netflix_facts', column_dict=column_dict)
        for _, row in fact_table.iterrows():
            self.db.insert_into_table(table_name='netflix_facts', column_names=column_dict.keys(), 
                                values=row.values.tolist())
        self.db.set_primary_key(table_name='netflix_facts', column_names=['show_id'])

    # create show dimension table
    def show_dimension_table(self):
        show_dimension_column_dict = {'show_id':'varchar', 'description':'varchar',
                                    'title':'varchar', 'type':'varchar', 
                                    'rating':'varchar', 'minutes':'int', 
                                        'seasons':'int'}
        show_dimension_table = self.netflix_data.copy()
        dp.create_content_dimension(show_dimension_table)
        show_dimension_table = show_dimension_table[show_dimension_column_dict.keys()].replace('', np.nan)
        self.db.create_table(table_name='show_dimension', column_dict=show_dimension_column_dict)
        for _, row in show_dimension_table.iterrows():
            self.db.insert_into_table(table_name='show_dimension', column_names=show_dimension_column_dict.keys(), 
                                values=row.values.tolist())
        self.db.set_primary_key(table_name='show_dimension', column_names=['show_id'])

    # create actor dimension table
    def actor_dimension_table(self):
        actor_dimension_table = self.netflix_data.copy()
        actor_dimension_column_dict = {'actor_id':'int', 'actor':'varchar'}
        actor_dimension_table.rename(columns={'cast':'actor'}, inplace=True)

        actor_dimension_table = dp.split_and_assign_id(actor_dimension_table,'actor','actor_id')
        actor_dimension_table = actor_dimension_table[actor_dimension_column_dict.keys()].replace('', np.nan).drop_duplicates()
        self.db.create_table(table_name='actor_dimension', column_dict=actor_dimension_column_dict)
        for _, row in actor_dimension_table.iterrows():
            self.db.insert_into_table(table_name='actor_dimension', column_names=actor_dimension_column_dict.keys(), 
                                values=row.values.tolist())
        self.db.set_primary_key(table_name='actor_dimension', column_names=['actor_id'])

    # create movie actors bridge table
    def movie_actors_bridge_table(self):
        movie_actor_dimension_table = self.netflix_data.copy().rename(columns={'cast':'actor'})
        movie_actor_dimension_column_dict = {'actor_id':'int', 'show_id':'varchar', 'movie_actor_id':'int'}
        movie_actor_dimension_table = dp.split_and_assign_id(movie_actor_dimension_table,'actor','actor_id', movie_col='show_id')
        movie_actor_dimension_table = movie_actor_dimension_table[['actor_id', 'show_id']].replace('', np.nan).drop_duplicates()
        movie_actor_dimension_table['movie_actor_id'] = range(1, len(movie_actor_dimension_table) + 1)
        self.db.create_table(table_name='movie_actor_dimension', column_dict=movie_actor_dimension_column_dict)
        for _, row in movie_actor_dimension_table.iterrows():
            self.db.insert_into_table(table_name='movie_actor_dimension', column_names=['actor_id', 'show_id', 'movie_actor_id'], 
                                values=row.values.tolist())
        self.db.set_primary_key(table_name='movie_actor_dimension', column_names=['movie_actor_id'])

    # create genre dimension table
    def genre_dimension_table(self):
        genre_dimension_table = self.netflix_data.copy().rename(columns={'listed_in':'genre'})
        genre_dimension_column_dict = {'genre_id':'int', 'genre':'varchar'}
        self.db.create_table(table_name='genre_dimension', column_dict=genre_dimension_column_dict)

        genre_dimension_table = dp.split_and_assign_id(genre_dimension_table,'genre','genre_id')
        genre_dimension_table = genre_dimension_table[genre_dimension_column_dict.keys()].replace('', np.nan).drop_duplicates()
        self.db.create_table(table_name='genre_dimension', column_dict=genre_dimension_column_dict)
        for _, row in genre_dimension_table.iterrows():
            self.db.insert_into_table(table_name='genre_dimension', column_names=genre_dimension_column_dict.keys(), 
                                values=row.values.tolist())
        self.db.set_primary_key(table_name='genre_dimension', column_names=['genre_id'])

    # create genre bridge table
    def genre_bridge_table(self):
        movie_genre_dimension_table = self.netflix_data.copy().rename(columns={'listed_in':'genre'})
        movie_genre_dimension_column_dict = {'genre_id':'int', 'show_id':'varchar', 'movie_genre_id':'int'}
        movie_genre_dimension_table = dp.split_and_assign_id(movie_genre_dimension_table,'genre','genre_id', movie_col='show_id')
        movie_genre_dimension_table = movie_genre_dimension_table[['genre_id', 'show_id']].replace('', np.nan).drop_duplicates()
        movie_genre_dimension_table['movie_genre_id'] = range(1, len(movie_genre_dimension_table) + 1)
        self.db.create_table(table_name='movie_genre_dimension', column_dict=movie_genre_dimension_column_dict)
        for _, row in movie_genre_dimension_table.iterrows():
            self.db.insert_into_table(table_name='movie_genre_dimension', column_names=['genre_id', 'show_id', 'movie_genre_id'], 
                                values=row.values.tolist())
        self.db.set_primary_key(table_name='movie_genre_dimension', column_names=['movie_genre_id'])

    # create type dimension table
    def type_dimension_table(self):
        type_dimension_table = self.netflix_data.copy()
        type_dimension_column_dict = {'type_id':'int', 'type':'varchar'}
        self.db.create_table(table_name='type_dimension', column_dict=type_dimension_column_dict)
        type_dimension_table = dp.split_and_assign_id(type_dimension_table,'type','type_id')
        type_dimension_table = type_dimension_table[type_dimension_column_dict.keys()].replace('', np.nan).drop_duplicates()
        for _, row in type_dimension_table.iterrows():
            self.db.insert_into_table(table_name='type_dimension', column_names=type_dimension_column_dict.keys(), 
                                values=row.values.tolist())
        self.db.set_primary_key(table_name='type_dimension', column_names=['type_id'])

    # create director dimension table
    def director_dimension_table(self):
        director_dimension_table = self.netflix_data.copy()
        director_dimension_column_dict = {'director_id':'int', 'director':'varchar'} 
        self.db.create_table(table_name='director_dimension', column_dict=director_dimension_column_dict)
        director_dimension_table = dp.split_and_assign_id(director_dimension_table,'director','director_id')
        director_dimension_table = director_dimension_table[director_dimension_column_dict.keys()].replace('', np.nan).drop_duplicates()
        for _, row in director_dimension_table.iterrows():
            self.db.insert_into_table(table_name='director_dimension', column_names=director_dimension_column_dict.keys(), 
                                values=row.values.tolist())
        self.db.set_primary_key(table_name='director_dimension', column_names=['director_id'])

    # create director bridge table
    def movie_director_dimension_table(self):
        movie_director_dimension_table = self.netflix_data.copy()
        movie_director_dimension_column_dict = {'director_id':'int', 'show_id':'varchar', 'movie_director_id':'int'}
        movie_director_dimension_table = dp.split_and_assign_id(movie_director_dimension_table,'director','director_id', movie_col='show_id')
        movie_director_dimension_table = movie_director_dimension_table[['director_id', 'show_id']].replace('', np.nan).drop_duplicates()
        movie_director_dimension_table['movie_director_id'] = range(1, len(movie_director_dimension_table) + 1)
        self.db.create_table(table_name='movie_director_dimension', column_dict=movie_director_dimension_column_dict)
        for _, row in movie_director_dimension_table.iterrows():
            self.db.insert_into_table(table_name='movie_director_dimension', column_names=['director_id', 'show_id', 'movie_director_id'], 
                                values=row.values.tolist())
        self.db.set_primary_key(table_name='movie_director_dimension', column_names=['movie_director_id'])

    # create country dimension table
    def country_dimension_table(self):
        country_dimension_table = self.netflix_data.copy()
        country_dimension_column_dict = {'country_id':'int', 'country':'varchar'}
        self.db.create_table(table_name='country_dimension', column_dict=country_dimension_column_dict)
        country_dimension_table = dp.split_and_assign_id(country_dimension_table,'country','country_id')
        country_dimension_table = country_dimension_table[country_dimension_column_dict.keys()].replace('', np.nan).drop_duplicates()
        for _, row in country_dimension_table.iterrows():
            self.db.insert_into_table(table_name='country_dimension', column_names=country_dimension_column_dict.keys(), 
                                values=row.values.tolist())
        self.db.set_primary_key(table_name='country_dimension', column_names=['country_id'])

    # create country bridge table
    def country_bridge_table(self):
        movie_country_dimension_table = self.netflix_data.copy()
        movie_country_dimension_column_dict = {'country_id':'int', 'show_id':'varchar', 'movie_country_id':'int'}
        movie_country_dimension_table = dp.split_and_assign_id(movie_country_dimension_table,'country','country_id', movie_col='show_id')
        movie_country_dimension_table = movie_country_dimension_table[['country_id', 'show_id']].replace('', np.nan).drop_duplicates()
        movie_country_dimension_table['movie_country_id'] = range(1, len(movie_country_dimension_table) + 1)
        self.db.create_table(table_name='movie_country_dimension', column_dict=movie_country_dimension_column_dict)
        for _, row in movie_country_dimension_table.iterrows():
            self.db.insert_into_table(table_name='movie_country_dimension', column_names=['country_id', 'show_id', 'movie_country_id'], 
                                values=row.values.tolist())
        self.db.set_primary_key(table_name='movie_country_dimension', column_names=['movie_country_id'])