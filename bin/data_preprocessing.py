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
    def __init__(self, path_to_file) -> None:
        self.path_to_file = path_to_file
        self.dataset = self.create_dataframe()
    
    def create_dataframe(self):
        return pd.read_csv(self.path_to_file) 
    
    def take_comma_separated_values_column_and_create_row_for_each_value(self, column_name):
        filtered_data = self.dataset[column_name]
        value_list = []
        
        for value in filtered_data:
            split_values = value.split(', ')  # Split the values by comma
            value_list.extend(split_values)   # Extend the list with the split values

        return list(set(value_list))
    
    def add_id_to_a_column(self, list_of_values: list, id_col_name='id', value_col_name='value'):
        return [{id_col_name: idx + 1, value_col_name: value} for idx, value in enumerate(list_of_values)]
       
        
        