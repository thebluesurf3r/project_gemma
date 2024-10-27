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
def analyze_keywords(keywords=None):
    jdbc_url = 'jdbc:postgresql://localhost:5432/project_gemma'
    table_name = 'cagliostro_gutenberg'
    df = load_data_spark(jdbc_url, table_name)

    if df is None or df.empty:
        logging.warning("Loaded DataFrame is empty or None.")
        return pd.DataFrame()

    # Ensure 'paragraph' column exists
    if 'paragraph' not in df.columns or df['paragraph'].isnull().all():
        logging.error("'paragraph' column is missing or contains only NaN values.")
        return pd.DataFrame()

    # Count occurrences of each keyword in the paragraphs
    keyword_counts = {keyword: 0 for keyword in keywords} if keywords else {}
    for paragraph in df['paragraph'].dropna():
        for keyword in keywords:
            keyword_counts[keyword] += paragraph.lower().count(keyword.lower())

    # Convert counts to DataFrame and filter out zero-count keywords
    keyword_counts_df = pd.DataFrame(keyword_counts.items(), columns=['keyword', 'count'])
    keyword_counts_df = keyword_counts_df[keyword_counts_df['count'] > 0]

    # Sort by count for consistent slider operation
    keyword_counts_df = keyword_counts_df.sort_values(by='count').reset_index(drop=True)

    # Create frames for each count threshold
    max_count = int(keyword_counts_df['count'].max())
    frames = []
    for i in range(max_count + 1):
        filtered_df = keyword_counts_df[keyword_counts_df['count'] <= i]
        frames.append(go.Frame(
            data=[go.Bar(
                x=filtered_df['keyword'],
                y=filtered_df['count'],
                text=filtered_df['count'],
                marker=dict(
                    color=filtered_df['count'],
                    coloraxis="coloraxis"  # Link to the global color axis
                )
            )],
            name=str(i)
        ))

    # Set up the initial figure
    fig = go.Figure(
        data=[go.Bar(
            x=keyword_counts_df['keyword'],
            y=keyword_counts_df['count'],
            text=keyword_counts_df['count'],
            marker=dict(
                color=keyword_counts_df['count'],
                coloraxis="coloraxis"
            )
        )],
        frames=frames
    )

    # Add a 'jet' color scale to the layout
    fig.update_layout(
        title="Keyword Frequency with a Count Limiter",
        xaxis_title="Keyword",
        yaxis_title="Count",
        font=dict(family="Arial", size=14, color="white"),
        plot_bgcolor="black",
        paper_bgcolor="black",
        coloraxis=dict(
            colorscale="Jet",  # Jet color scale for the color axis
            colorbar=dict(title="Count", tickvals=[0, max_count])
        ),
        sliders=[{
            "active": max_count,
            "currentvalue": {"prefix": "Max Count Threshold: ", "font": {"color": "white", "size": 14}},
            "pad": {"t": 50},
            "steps": [
                {"method": "animate", "label": str(i), 
                 "args": [[str(i)], {"frame": {"duration": 2000, "redraw": True}, "mode": "immediate", "transition": {"duration": 200, "easing": "cubic-in-out"}}]}
                for i in range(max_count + 1)
            ]
        }]
    )

    fig.update_traces(texttemplate='%{text}', textposition='outside')
    fig.show()
    logging.info("Keyword frequency analysis with native slider and jet color scale is complete.")