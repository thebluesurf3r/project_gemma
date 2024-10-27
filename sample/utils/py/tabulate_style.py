#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from google.colab import drive
drive.mount('/content/drive')

import pandas as pd
import logging
import numpy as np
from logging_configuration import setup_logging
from tabulate import tabulate

import sys
sys.path.append('/content/drive/MyDrive/git/utils/py/')
from logging_configuration import setup_logging
#=====================================================================================================================================================#
#== Configure logging with specified log file and level ==#
logger = setup_logging(log_file='app_log.log', log_level=logging.INFO)


# In[ ]:


def tab_fmt(data, n, style='psql'):
    """
    Logs the provided data in a formatted table.

    Parameters:
    - data: DataFrame, Series, NumPy array, or any 2D iterable to log.
    - n: int, the number of rows to display.
    - style: str, the table format style for tabulate (default is 'psql').
    """
    try:
        # Check if the input is a DataFrame
        if isinstance(data, pd.DataFrame):
            # Limit to first n rows for logging
            data = data.head(n)
            logging.info(tabulate(data, headers='keys', tablefmt=style))

        # Check if the input is a Series
        elif isinstance(data, pd.Series):
            logging.info(tabulate(data.head(n).reset_index().values,
                                  headers=data.head(n).reset_index().columns,
                                  tablefmt=style))

        # Check if the input is a NumPy array
        elif isinstance(data, np.ndarray):
            # Limit to first n rows for logging
            logging.info(tabulate(data[:n], tablefmt=style))

        # Check if the input is a simple list of lists (matrix)
        elif isinstance(data, (list, tuple)) and all(isinstance(i, (list, tuple)) for i in data):
            # Limit to first n rows for logging
            logging.info(tabulate(data[:n], tablefmt=style))

        else:
            logging.warning("Unsupported data type provided for logging.")

    except Exception as e:
        logging.error(f"Error in tabulating data: {str(e)}")

