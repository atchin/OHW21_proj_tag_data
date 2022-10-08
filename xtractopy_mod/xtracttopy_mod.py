# necessary packages
import datetime as dt
import xarray as xr
import numpy as np
import pandas as pd
from typing import Dict, Union
import fsspec
import matplotlib.pyplot as plt
from datetime import datetime
import logging
from abc import ABC

class logger(ABC):
    """
    Abstract base class for creating a logger for each class created
    """
    def _create_logger(self):
        """
        :return: private function that returns a logger
        """
        # Gets or creates a logger
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        logger = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
        return logger

class XArrayHelper(logger):
    def __init__(self, max_long,min_long):
        self.max_long = max_long
        self.min_long = min_long
        logger = self._create_logger()

    def create_subset(self,large_data_set):
        """
        :param large_data_set: an Xarray of data
        :return: will take the max long and min long used and find them  in the subset and just return the working data
        """
        subset_lon = (large_data_set.lon >= self.min_long) & (large_data_set.lon <= self.max_long)
        subset_env_data = large_data_set.where(subset_lon, drop=True)
        return subset_env_data

    def flip_table(self):
        """
        goal is to make each col a row in a pandas or spark dataframe so we can analyze data
        :return:
        """
        pass







class extractor():
    def __init__(self, file_name,tag_data,env_data, col_mapper =  {"lat": "lat", "lon": "lon", "datetime": "time"}):
        '''
        :param file_name: Is a string of the tag data
        :param tag_data: pandas df of lat and long points
        :param env_data: xarray
        '''
        self.file_name = file_name
        self.tag_data = tag_data
        self.env_data = env_data
        self.col_mapper = col_mapper


    def fuction_dataset_point(**kwargs) -> Dict[str, Union[float, int]]:
        '''function to insure the return type '''
        pass

    def extract_xarr_to_df(self):

        def map_row_to_keys(row) -> Dict[str, Union[float, int]]:
            extract_coordinates = { val: row[key]  for key,val in self.col_mapper.items()}
            return extract_coordinates

        return self.tag_data.apply(
            lambda row: map_row_to_keys(row), axis=1, result_type="expand"
        )





