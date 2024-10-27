#!/usr/bin/env python
# coding: utf-8

# In[13]:


from google.colab import drive
drive.mount('/content/drive')

import os
from tabulate import tabulate
import pandas as pd
import logging

import sys
sys.path.append('../content/drive/MyDrive/git/utils/py')
from logging_configuration import setup_logging
from load_from_csv import load_data

#=====================================================================================================================================================#
#== Configure logging with specified log file and level ==#

logger = setup_logging(log_file='app_log.log', log_level=logging.INFO)

#=====================================================================================================================================================#
#== Function to Convert Specified Columns of DataFrame to Object dtype ==#

def convert_columns_to_object(df, column_indices):
    """
    Convert specified columns of a DataFrame to object dtype.

    Parameters:
    - df: Pandas DataFrame containing the data.
    - column_indices: List of indices of columns to convert to object dtype.
    """
    df = load_data()
    # Check if the provided column_indices are valid
    for idx in column_indices:
        if idx < 0 or idx >= df.shape[1]:
            logger.warning(f"Index {idx} is out of bounds for DataFrame with {df.shape[1]} columns.")
            continue  # Skip invalid indices

        # Fill NaNs with empty strings before converting
        try:
            df.iloc[:, idx] = df.iloc[:, idx].fillna('').astype('object')  # Attempt to convert to object dtype
            logger.info(f"Column at index {idx} converted to object dtype.")
        except Exception as e:
            logger.error(f"Failed to convert column at index {idx} to object dtype: {str(e)}")  # Log conversion errors


# ### Call the function

# In[14]:


#df = load_data()

#=====================================================================================================================================================#
#== Convert Columns n1 to n2 of DataFrame ==#

#== Specify column indices to convert ==#
#column_indices_to_convert = list(range(1, 7))  # Indices from 1 to 6 (inclusive of 1, exclusive of 7)

# Convert specified columns
#convert_columns_to_object(df, column_indices_to_convert)

#=====================================================================================================================================================#
#== Check and Display the DataFrame dtypes After Conversion ==#

# Reset index for better display of DataFrame dtypes
#data_types_to_convert = df.dtypes.reset_index()
#data_types_to_convert.columns = ['Column', 'Data Type']  # Rename columns for clarity

# Data types in a formatted table
#logging.info(tabulate(data_types_to_convert, headers='keys', tablefmt='psql'))

