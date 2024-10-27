import pandas as pd
import logging
import os
import sys
from tabulate import tabulate
import plotly.io as pio
import plotly.graph_objects as go
import plotly.express as px
from nltk.sentiment import SentimentIntensityAnalyzer

sys.path.append(os.path.abspath(os.path.join('..', 'utils')))  # Adjust the path based on your structure
from logging_configuration import setup_logging
from load_from_csv import load_data
from tabulate_style import tab_fmt
from explode_tokens import explode_to_tokens
from custom_plotly_template import get_custom_layout, set_custom_template

# Call the function to set the custom template
set_custom_template()

# Initialize Sentiment Intensity Analyzer
sia = SentimentIntensityAnalyzer()

def analyze_token_sentiment(tokens_df, sentiment_types=['compound'], return_type='dataframe'):
    """
    Analyzes sentiment for tokens in tokens_df using NLTK's VADER sentiment analysis.

    Parameters:
    - tokens_df: DataFrame containing individual tokens in a 'paragraph' column.
    - sentiment_types: list of str, sentiment scores to calculate, e.g., ['compound', 'pos'].
    - return_type: str, 'dataframe', 'plot', or 'both' for output preference.

    Returns:
    - DataFrame with sentiment scores or None based on return_type.
    """
    # Ensure tokens_df has the correct structure
    if 'paragraph' not in tokens_df.columns:
        logging.error("The DataFrame must contain a 'paragraph' column.")
        return None

    # Filter out non-alphabetic tokens and empty strings
    tokens_df = tokens_df[tokens_df['paragraph'].str.isalpha() & tokens_df['paragraph'].str.strip().astype(bool)].copy()

    # Calculate the length (character count) of each token
    tokens_df['length'] = tokens_df['paragraph'].str.len()

    # Initialize sentiment score columns
    for sentiment_type in sentiment_types:
        tokens_df[sentiment_type] = None  # Initialize columns

    # Apply sentiment analysis to each token with error handling
    for i, token in enumerate(tokens_df['paragraph']):
        try:
            scores = sia.polarity_scores(token)
            for sentiment_type in sentiment_types:
                tokens_df.at[i, sentiment_type] = scores[sentiment_type]
        except Exception as e:
            logging.error(f"Error calculating sentiment for token index {i}: {e}")
            for sentiment_type in sentiment_types:
                tokens_df.at[i, sentiment_type] = None

    # Calculate the 95th percentile for each sentiment type and log the values
    for sentiment_type in sentiment_types:
        if sentiment_type in tokens_df.columns:
            percentile_95_sentiment = round(tokens_df[sentiment_type].quantile(0.95), 2)
            logging.info(f"95th Percentile Sentiment Score ({sentiment_type}): {percentile_95_sentiment}")

            # Filter tokens with sentiment >= 95th percentile
            high_sentiment_tokens = tokens_df[tokens_df[sentiment_type] >= percentile_95_sentiment]
            logging.info("Top tokens by sentiment score:\n")
            logging.info(high_sentiment_tokens.head())

            # Plot sentiment distribution if required
            if return_type in ['plot', 'both']:
                fig = px.histogram(tokens_df, x=sentiment_type, nbins=30,
                                   title=f'Distribution of {sentiment_type.capitalize()} Sentiment Scores',
                                   labels={sentiment_type: 'Sentiment Score', 'count': 'Count'},
                                   color_discrete_sequence=px.colors.sequential.Jet)  # Using 'Jet' color scale
                fig.show()

    return tokens_df if return_type in ['dataframe', 'both'] else None