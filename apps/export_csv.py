import os
import pandas as pd
import logging

# Reuse the initialized logger from the previous setup
logger = logging.getLogger("load_data_spark_logger")

def export_to_csv(df, file_path='exported_data.csv'):
    """
    Export a Pandas DataFrame to a CSV file.

    Parameters:
    - df (pd.DataFrame): The DataFrame to export.
    - file_path (str): The path where the CSV file will be saved.

    Returns:
    - bool: True if export is successful, False otherwise.
    """
    if df is None or df.empty:
        logger.error("DataFrame is empty or None. Cannot export to CSV.")
        return False

    try:
        df.to_csv(file_path, index=False)
        logger.info(f"Data successfully exported to {file_path}.")
        return True

    except Exception as e:
        logger.error(f"Error exporting DataFrame to CSV: {e}")
        return False

# Usage with load_data_spark output:
df = load_data_spark('jdbc:postgresql://localhost:5432/project_gemma', 'cagliostro_gutenberg')
if df is not None:
    export_success = export_to_csv(df, 'cagliostro_gutenberg_export.csv')
