import os
from tabulate import tabulate
import pandas as pd
import logging

# Import custom logging configuration and log DataFrame
from logging_configuration import setup_logging

#=====================================================================================================================================================#
#== Configure logging with specified log file and level ==#

logger = setup_logging(log_file='app_log.log', log_level=logging.INFO)

#=====================================================================================================================================================#
#== Function to Load Dataset from CSV File ==#

def load_data():
    """
    Load a CSV dataset from the specified file path and log details about the dataset.
    
    Returns:
        DataFrame: The loaded DataFrame, or None if loading fails.
    """
    file_name = "cagliostro_gutenberg.csv"  # Specify the CSV file name
    file_path = os.path.join("..", "csv", file_name)  # Construct the full file path

    try:
        # Check if the file exists
        if not os.path.exists(file_path):
            logger.error(f"File not found: {file_path}")  # Log error if file is missing
            return None

        # Load the dataset into a DataFrame
        df = pd.read_csv(file_path)
        
        # Check if the DataFrame is empty
        if df.empty:
            logger.warning(f"{file_name} is empty. No data to load.")  # Log warning for empty DataFrame
            return None

        # Log successful import of the dataset
        logger.info(f"{file_name} imported successfully!")
        logger.info(f"There are {df.shape[0]} rows and {df.shape[1]} columns.")  # Log DataFrame dimensions

        # Export schema of the DataFrame
        schema = df.dtypes.reset_index()  # Get data types and reset index
        schema.columns = ['Column Name', 'Data Type']  # Rename columns for clarity

        # Get unique values for each column
        unique_values = df.nunique()  # Count unique values in each column
        schema['n_unique'] = unique_values.values  # Add unique counts to the schema DataFrame

        # Log the schema of the loaded dataset
        logger.info("Schema of the loaded dataset:")
        logger.info(f"\n{tabulate(schema, headers='keys', tablefmt='psql')}")  # Log formatted schema

        # Handle columns with zero unique values
        zero_unique_cols = schema[schema['n_unique'] == 0]['Column Name'].tolist()
        for col in zero_unique_cols:
            logger.warning(f"Column '{col}' has no unique values and will be dropped.")  # Log warning
            df.drop(columns=col, inplace=True)  # Drop the column

        return df  # Return the loaded DataFrame

    except pd.errors.EmptyDataError:
        logger.error("The file is empty or contains no data.")  # Log error for empty file
    except pd.errors.ParserError:
        logger.error("Error parsing the CSV file. Please check the file format.")  # Log error for parsing issues
    except Exception as e:
        logger.error(f"An unexpected error occurred: {str(e)}")  # Log any other unexpected errors

    # Return None if any error occurs
    return None

#=====================================================================================================================================================#
#== Load the Data and Store it in a DataFrame ==#

#df = load_data()