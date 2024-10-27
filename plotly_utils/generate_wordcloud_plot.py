from wordcloud import WordCloud
import pandas as pd
import numpy as np
import logging
#import os
import sys
#from tabulate import tabulate
#import plotly.io as pio
#import plotly.graph_objects as go
#import plotly.express as px


sys.path.append(os.path.abspath(os.path.join('..', 'utils')))  # Adjust the path based on your structure
from logging_configuration import setup_logging
from load_from_csv import load_data
from token_distribution import analyze_distribution
from tabulate_style import tab_fmt

df = load_data()

token_counts = analyze_distribution(df, lower_bound=50, upper_bound=1500, keywords=None, return_type='dataframe')

#=====================================================================================================================================================#
#== Analyze token distribution and stats ==#

def generate_word_cloud(token_counts, lower_bound=None, upper_bound=None):

    # Ensure that the count column is of integer type and filter out non-positive counts
    token_counts['count'] = token_counts['count'].astype(int)
    token_counts = token_counts[token_counts['count'] > 0]

    # Filter tokens based on user-defined lower and upper bounds
    token_counts = token_counts[(token_counts['count'] >= lower_bound) & (token_counts['count'] <= upper_bound)]

    # Check if token_counts is empty after filtering
    if token_counts.empty:
        logging.warning("No tokens found within the specified bounds.")
        return

    # Concatenate tokens into a single string based on their frequencies
    text = ' '.join([token for token, count in zip(token_counts['token'], token_counts['count']) for _ in range(count)])

    # Generate the word cloud image
    wordcloud = WordCloud(width=1400, height=800, background_color='black', colormap='viridis').generate(text)

    # Convert the word cloud image to a NumPy array
    image_array = np.array(wordcloud)

    # Create a Plotly figure
    fig = go.Figure()

    # Add the word cloud image as a trace
    fig.add_trace(go.Image(z=image_array))

    # Update layout to format the word cloud
    fig.update_layout(title='Word Cloud',
                      #height=800,
                      #width=1400,
                      xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                      yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                      template='plotly_dark')

    # Show the word cloud
    fig.show()

    logging.info("Word cloud has been displayed successfully!")

# Usage (after analyzing tokens):
#generate_word_cloud(token_counts, lower_bound=50, upper_bound=5000)