import pandas as pd
import logging
from logging_configuration import setup_logging
from load_from_csv import load_data
from tabulate import tabulate

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

#df = load_data()

#=====================================================================================================================================================#
#== Explode the Specified Column into Tokens ==#
#tokens_df = explode_to_tokens(df, column='paragraph')

#=====================================================================================================================================================#
#== Preview the First Few Tokens ==#
#preview_token = tokens_df.head(10)  # Keep as a Series for correct formatting

#=====================================================================================================================================================#
#== Log the First Few Tokens in a Formatted Table ==#
#logger.info(tabulate(preview_token.reset_index(drop=True).to_frame(), headers=['token'], tablefmt='psql'))  # Display the first few tokens