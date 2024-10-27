import os
import pandas as pd
import logging
from pyspark.sql import SparkSession
from tabulate import tabulate

spark = SparkSession.builder \
    .appName("MyApp") \
    .getOrCreate()

# Set log level to INFO
spark.sparkContext.setLogLevel("INFO")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_data_spark(jdbc_url, table_name):
    """
    Load data from PostgreSQL into a Pandas DataFrame using PySpark.

    Parameters:
    - jdbc_url (str): The JDBC URL for the PostgreSQL database.
    - table_name (str): The name of the table to load data from.

    Returns:
    - pd.DataFrame: Data loaded into a Pandas DataFrame.
    """
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')

    if not db_user or not db_password:
        logging.error("Database credentials not found in environment variables.")
        return None

    try:
        # Initialize Spark session
        spark = SparkSession.builder \
            .appName("Load Data from PostgreSQL") \
            .config("spark.jars", "/home/tron/git/project_gemma/jdbc/postgresql-42.7.4.jar") \
            .getOrCreate()

        properties = {
            'user': db_user,
            'password': db_password,
            'driver': 'org.postgresql.Driver'
        }

        df_spark = spark.read.jdbc(url=jdbc_url, table=table_name, properties=properties)
        logging.info(f"Data from {table_name} imported successfully!")
        logging.info(f"There are {df_spark.count()} rows and {len(df_spark.columns)} columns.")

        df_pandas = df_spark.toPandas()

        # Export schema
        schema = pd.DataFrame({
            'Column Name': df_spark.columns,
            'Data Type': [str(dtype) for dtype in df_spark.dtypes]
        })

        unique_values = {col: df_spark.select(col).distinct().count() for col in df_spark.columns}
        schema['n_unique'] = schema['Column Name'].map(unique_values)

        logging.info("Schema of the loaded dataset:")
        logging.info(f"{tabulate(schema, headers='keys', tablefmt='psql')}")

        return df_pandas

    except Exception as e:
        logging.error(f"Error loading data: {e}")
        return None

# Usage:
#df = load_data_spark('jdbc:postgresql://localhost:5432/project_gemma', 'cagliostro_gutenberg')