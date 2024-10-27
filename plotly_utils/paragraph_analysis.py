import pandas as pd
import logging
import os
import sys
from tabulate import tabulate
import plotly.io as pio
import plotly.graph_objects as go
import plotly.express as px


sys.path.append(os.path.abspath(os.path.join('..', 'utils')))  # Adjust the path based on your structure
from logging_configuration import setup_logging
from load_from_csv import load_data
from tabulate_style import tab_fmt
from custom_plotly_template import get_custom_layout, set_custom_template

# Call the function to set the custom template
set_custom_template()

from nltk.sentiment import SentimentIntensityAnalyzer
import pandas as pd

# Initialize the Sentiment Intensity Analyzer
sia = SentimentIntensityAnalyzer()

# Function to perform sentiment analysis on tokens
def analyze_sentiment(tokens_df):
    """
    Performs sentiment analysis on a DataFrame of tokens.

    Parameters:
    - tokens_df (pd.DataFrame): DataFrame containing tokens to analyze.

    Returns:
    - pd.DataFrame: DataFrame containing tokens and their sentiment scores.
    """
    # Apply sentiment analysis to each token
    sentiment_scores = tokens_df.apply(lambda token: sia.polarity_scores(token))

    # Convert the sentiment scores into a DataFrame
    sentiment_df = pd.DataFrame(list(sentiment_scores))

    # Combine the tokens with their sentiment scores
    result_df = pd.concat([tokens_df.reset_index(drop=True), sentiment_df], axis=1)
    result_df.columns = ['token', 'neg', 'neu', 'pos', 'compound']  # Rename columns for clarity
    
    return result_df

# Example usage after exploding tokens
tokens_df = explode_to_tokens(df, column='paragraph')
sentiment_result_df = analyze_sentiment(tokens_df)

# Display sentiment analysis result
print(sentiment_result_df.head())
