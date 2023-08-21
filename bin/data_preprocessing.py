import os
import logging
import logging.config
import pandas as pd

# Get the absolute directory of the current script
abs_dir = os.path.dirname(os.path.abspath(__file__))

# Construct the absolute path to the logging configuration file
logging_config_path = os.path.join(abs_dir, '..', 'util', 'logging_to_file.conf')

logging.config.fileConfig(fname=logging_config_path)

# Get the custom Logger from Configuration File
logger = logging.getLogger(__name__)

class NetflixDataset:
    def __init__(self, path_to_file):
        self.path_to_file = path_to_file
        self.dataset = self.create_dataframe()
    
    def create_dataframe(self):
        return pd.read_csv(self.path_to_file) 
    
    def take_comma_separated_values_column_and_create_row_for_each_value(self, column_name: str):
        filtered_data = self.dataset[column_name]
        value_list = []
        
        for value in filtered_data:
            split_values = str(value).split(', ')  # Split the values by comma
            value_list.extend(split_values)   # Extend the list with the split values

        return list(set(value_list))
    
    @staticmethod
    def add_id_to_a_column(list_of_values: list, id_col_name='id', value_col_name='value'):
        return [{id_col_name: idx + 1, value_col_name: value} for idx, value in enumerate(list_of_values)]
    
    @property
    def get_dataset(self):
        return self.dataset

def encode_column(df: pd.DataFrame, column_name: str):
    # Use factorize to encode the unique values
    encoded_values, unique_values = pd.factorize(df[column_name])

    # Increment the encoded values by 1
    encoded_values += 1

    # Update the specified column in the DataFrame with the encoded values
    df[column_name] = encoded_values

def create_content_dimension(df: pd.DataFrame):
    # Create a function to separate duration into seasons and minutes
    def separate_duration(value):
        if "Season" in str(value):
            return str(value).split(' ')[0], None
        elif "min" in str(value):
            return None, str(value).split(' ')[0]
        else:
            return None, None

    # Apply the separate_duration function to the duration column
    df['seasons'], df['minutes'] = zip(*df['duration'].apply(separate_duration))

    # Drop the original duration column
    df.drop('duration', axis=1, inplace=True)

    # Create a new column for content_id
    df['content_id'] = range(1, len(df) + 1)

import pandas as pd

import numpy as np

import pandas as pd
import numpy as np

def split_and_assign_id(df: pd.DataFrame, names_col: str, id_col_name='id', movie_col=None):
    # Splitting the names column into multiple rows
    s = df[names_col].str.split(', ').apply(pd.Series, 1).stack()
    s.index = s.index.droplevel(-1)
    s.name = names_col
    df_split = df.drop(names_col, axis=1).join(s).reset_index(drop=True)

    # Sorting the unique names (excluding NaN)
    unique_names = sorted([name for name in df_split[names_col].unique() if isinstance(name, str)])
    
    # Creating a mapping from unique values (including NaN) to IDs
    name_to_id = {name: idx + 1 for idx, name in enumerate(unique_names)}
    name_to_id[np.nan] = len(unique_names) + 1

    # Applying the mapping to the 'name' column
    df_split[id_col_name] = df_split[names_col].map(name_to_id)

    if movie_col:
        df_split[movie_col] = df_split[movie_col]

    return df_split

