import pandas as pd
import logging
import numpy as np
from logging_configuration import setup_logging
from tabulate import tabulate

#=====================================================================================================================================================#
#== Configure logging with specified log file and level ==#
logger = setup_logging(log_file='app_log.log', log_level=logging.INFO)

def tab_fmt(df, n, style=None):
    """
    Logs the provided df in a formatted table.

    Parameters:
    - df: DataFrame, Series, NumPy array, or any 2D iterable to log.
    - n: int, the number of rows to display.
    - style: str, the table format style for tabulate (default is 'psql').
    """
    try:
        # Check if the input is a DataFrame
        if isinstance(df, pd.DataFrame):
            # Limit to first n rows for logging
            df = df.head(n)  
            logging.info(tabulate(df, headers='keys', tablefmt=style))
        
        # Check if the input is a Series
        elif isinstance(df, pd.Series):
            logging.info(tabulate(df.head(n).reset_index().values, 
                                  headers=df.head(n).reset_index().columns, 
                                  tablefmt=style))
        
        # Check if the input is a NumPy array
        elif isinstance(df, np.ndarray):
            # Limit to first n rows for logging
            logging.info(tabulate(df[:n], tablefmt=style))
        
        # Check if the input is a simple list of lists (matrix)
        elif isinstance(df, (list, tuple)) and all(isinstance(i, (list, tuple)) for i in df):
            # Limit to first n rows for logging
            logging.info(tabulate(df[:n], tablefmt=style))
        
        else:
            logging.warning("Unsupported df type provided for logging.")
    
    except Exception as e:
        logging.error(f"Error in tabulating df: {str(e)}")