from ipywidgets import widgets, interact
import plotly.graph_objects as go
import pandas as pd
import logging
from pyspark.sql import SparkSession
import sys
import os
import tabulate as tabulate

spark = SparkSession.builder \
    .appName("MyApp") \
    .getOrCreate()

# Set log level to INFO
spark.sparkContext.setLogLevel("INFO")

sys.path.append(os.path.abspath(os.path.join('..', 'apps')))
from data_loader import load_data_spark

# Set up logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Define the analyze_keywords function with fine-tuning sliders
def analyze_paragraphs(lower_bound=0, upper_bound=float('inf'), keywords=None):
    jdbc_url = 'jdbc:postgresql://localhost:5432/project_gemma'
    table_name = 'cagliostro_gutenberg'
    df = load_data_spark(jdbc_url, table_name)

    if df is None or df.empty:
        logging.warning("Loaded DataFrame is empty or None.")
        return pd.DataFrame()  # Return empty DataFrame if no data is available

    # Ensure 'paragraph' column exists and has valid entries
    if 'paragraph' not in df.columns or df['paragraph'].isnull().all():
        logging.error("'paragraph' column is missing or contains only NaN values.")
        return pd.DataFrame()

    # Explode the 'paragraph' column into tokens
    tokens_df = df['paragraph'].dropna().str.split(' ').explode().reset_index(drop=True)
    total_tokens = len(tokens_df)

    # Calculate the number of tokens in each paragraph
    token_counts = df['paragraph'].dropna().apply(lambda x: len(x.split()))

    # Combine the lengths and token counts into a single DataFrame
    stats_df = pd.DataFrame({'token_count': token_counts})

    # Filter token counts based on provided lower and upper bounds
    filtered_stats_df = stats_df[(stats_df['token_count'] >= lower_bound) & 
                                  (stats_df['token_count'] <= upper_bound)]

    # Create a DataFrame for token counts
    token_count_series = tokens_df.value_counts().reset_index(name='count')
    token_count_series.columns = ['token', 'count']  # Rename columns

    # If keywords are provided, filter the token counts
    if keywords is not None:
        token_count_series = token_count_series[token_count_series['token'].isin(keywords)]

    # Filter token counts again based on provided lower and upper bounds
    token_count_series = token_count_series[(token_count_series['count'] >= lower_bound) & 
                                            (token_count_series['count'] <= upper_bound)]

    # Logging for filtered data
    logging.info(f"Filtered stats_df:\n{tabulate(filtered_stats_df, headers='keys', tablefmt='psql')}")
    logging.info(f"Filtered token_count_series:\n{tabulate(token_count_series, headers='keys', tablefmt='psql')}")

    # Check for empty data
    if filtered_stats_df.empty:
        logging.warning("No data available for the specified token count range in filtered_stats_df.")
    if token_count_series.empty:
        logging.warning("No data available for the specified token count range in token_count_series.")
        return pd.DataFrame()  # Return empty DataFrame if no data is available

    # Calculate the 95th percentile and round to 2 decimal places
    percentile_95 = round(token_count_series['count'].quantile(0.95), 2)

    # Identify the token corresponding to the 95th percentile
    token_95 = token_count_series[token_count_series['count'] >= percentile_95]['token'].iloc[0]

    # Log the 95th percentile token
    logging.info(f"95th Percentile Token: {token_95} with Count: {percentile_95}")

    # Plot histogram of token counts
    fig_tokens = px.histogram(filtered_stats_df, x='token_count', nbins=50,
                               color='token_count',
                               title='Distribution of Token Counts',
                               labels={'token_count': 'Token Count',
                                       'count': 'Count'})

    # Plot bar chart for token counts
    fig_tokens_bar = px.bar(token_count_series, x='token', y='count',
                             title='Token Counts',
                             labels={'token': 'Token', 'count': 'Count'},
                             color='count')

    # Add 95th percentile line
    fig_tokens_bar.add_trace(go.Scatter(
        x=token_count_series['token'],  # Use the same x values as the bar chart
        y=[percentile_95] * len(token_count_series),  # Repeat the percentile value
        mode='lines',
        name='95th Percentile',
        line=dict(color='red', width=2, dash='dash')  # Customize the line style
    ))

    # Show the plots
    fig_tokens.show()
    fig_tokens_bar.show()

    logging.info(f"Total tokens extracted: {total_tokens}")
    logging.info("Paragraph analysis completed!")

    # Insert the 95th percentile token into PostgreSQL
    insert_into_postgres(token_95, percentile_95, lower_bound, upper_bound)

    return token_count_series  # Return the token counts

def insert_into_postgres(token_name, token_count, lower_bound, upper_bound):
    # Create an SQLAlchemy engine (update with your actual database URI)
    engine = create_engine('postgresql://postgres:password@localhost:5432/project_gemma')

    # Create a DataFrame for the token data, including lower and upper bounds
    token_data = pd.DataFrame({
        'token': [token_name],
        'count': [token_count],
        'lower_bound': [lower_bound],
        'upper_bound': [upper_bound]
    })

    try:
        # Insert into PostgreSQL (assuming the table 'percentile_tokens' exists)
        token_data.to_sql('percentile_tokens', engine, if_exists='append', index=False)
        logging.info(f"Inserted token '{token_name}' with count {token_count} into PostgreSQL.")
    except Exception as e:
        logging.error(f"Error inserting data into PostgreSQL: {e}")

# Usage

#token_counts = analyze_paragraphs(lower_bound=80, upper_bound=1500, keywords=None)