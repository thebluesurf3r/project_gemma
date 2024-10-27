#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from google.colab import drive
drive.mount('/content/drive')

import pandas as pd
import logging
from tabulate import tabulate

import sys
sys.path.append('/content/drive/MyDrive/git/utils/py/')
from logging_configuration import setup_logging
from load_from_csv import load_data
from tabulate_style import tab_fmt

#=====================================================================================================================================================#
#== Configure logging with specified log file and level ==#
logger = setup_logging(log_file='app_log.log', log_level=logging.INFO)


# In[ ]:


#=====================================================================================================================================================#
#== Function to Explode Specified Column of DataFrame into Tokens ==#
def explode_to_tokens(df, column='paragraph'):
    """
    Explodes the specified column of a DataFrame into tokens.

    Parameters:
    - df (pd.DataFrame): The input DataFrame containing the text data.
    - column (str): The column name to be exploded into tokens (default is 'paragraph').

    Returns:
    - pd.DataFrame: A DataFrame containing tokens, with each token in a separate row.
    """
    # Check if the specified column exists in the DataFrame
    if column not in df.columns:
        raise ValueError(f"Column '{column}' not found in DataFrame.")

    # Explode the specified column into tokens and reset the index
    tokens_df = df[column].str.split(' ').explode().reset_index(drop=True)

    return tokens_df


# ### Calling the function via tabulate_style

# In[ ]:


#=====================================================================================================================================================#
#== Preview the main dataframe ==#
df = load_data()


# In[ ]:


tab_fmt(df, 3, style='grid')


# In[ ]:


#=====================================================================================================================================================#
#== Explode the Specified Column into Tokens ==#
tokens_df = explode_to_tokens(df, column='paragraph')


# In[ ]:


tab_fmt(tokens_df, 5, style='grid')


# In[ ]:




