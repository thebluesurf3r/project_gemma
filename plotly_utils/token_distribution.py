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

#=====================================================================================================================================================#
#== Analyze token distribution and stats ==#
def analyze_distribution(df, lower_bound=0, upper_bound=float('inf'), keywords=None, return_type='dataframe'):
    """
    Analyzes paragraphs for token distribution and can return data or plots.

    Parameters:
    - df: DataFrame containing paragraphs.
    - lower_bound: int, minimum token count to include.
    - upper_bound: int, maximum token count to include.
    - keywords: list of str, specific tokens to filter.
    - return_type: str, 'dataframe', 'plot', or 'both' to specify output preference.

    Returns:
    - DataFrame of token counts or None based on return_type.
    """
    # Explode the 'paragraph' column into tokens
    tokens_df = df['paragraph'].str.split(' ').explode().reset_index(drop=True)

    # Count the total number of tokens
    total_tokens = tokens_df.count()

    # Calculate the number of tokens in each paragraph
    token_counts = df['paragraph'].apply(lambda x: len(x.split()))

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

    # Filter token counts based on provided lower and upper bounds
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
        return pd.DataFrame() if return_type in ['dataframe', 'both'] else None  # Return empty DataFrame if no data is available

    # Calculate the 95th percentile and round to 2 decimal places
    percentile_95 = round(token_count_series['count'].quantile(0.95), 2)

    # Identify the token corresponding to the 95th percentile
    token_95 = token_count_series[token_count_series['count'] >= percentile_95]['token'].iloc[0]

    # Log the 95th percentile token
    logging.info(f"95th Percentile Token: {token_95} with Count: {percentile_95}")

    # Prepare to return plots
    plots = []
    if return_type in ['plot', 'both']:
        # Plot histogram of token counts
        fig_tokens = px.histogram(filtered_stats_df, x='token_count', nbins=50, 
                                   color='token_count',
                                   title='Distribution of Token Counts',
                                   labels={'token_count': 'Token Count', 'count': 'Count'})
        plots.append(fig_tokens)

        # Plot bar chart for token counts
        fig_tokens_bar = px.bar(token_count_series, x='token', y='count',
                                 title='Token Counts',
                                 labels={'token': 'Token', 'count': 'Count'},
                                 color='count')

        # Add 95th percentile line
        fig_tokens_bar.add_trace(go.Scatter(
            x=token_count_series['token'],
            y=[percentile_95] * len(token_count_series),
            mode='lines',
            name='95th Percentile',
            line=dict(color='red', width=2, dash='dash')
        ))
        plots.append(fig_tokens_bar)

    # Show the plots if required
    if return_type in ['plot', 'both']:
        for plot in plots:
            plot.show()

    logging.info(f"Total tokens extracted: {total_tokens}")
    logging.info("Paragraph analysis completed!")

    return token_count_series if return_type in ['dataframe', 'both'] else None