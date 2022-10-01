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





