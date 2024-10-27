import os
import time
import logging
import pandas as pd
import numpy as np
import pandas as pd
import logging
import time
from queue import Queue
from threading import Thread
from logging.handlers import RotatingFileHandler

#=====================================================================================================================================================#
#== Define a global DataFrame to store log entries ==#

# This DataFrame will store all log entries, each consisting of a timestamp, logging level, and message.
log_df = pd.DataFrame(columns=['timestamp', 'level', 'message'])

#=====================================================================================================================================================#
#== Define a custom logging handler that adds log records to a DataFrame ==#

class DataFrameLoggingHandler(logging.Handler):
    """
    A logging handler that appends log records to a DataFrame for in-memory logging.
    Takes in a DataFrame and a queue to store log messages temporarily.
    """
    def __init__(self, dataframe, queue):
        super().__init__()
        self.dataframe = dataframe
        self.queue = queue

    def emit(self, record):
        """
        Emit a log record to the DataFrame by pushing it to the queue.
        """
        try:
            # Capture log details
            timestamp = pd.Timestamp.now()
            level = record.levelname
            message = record.getMessage()
            # Push to queue for processing
            self.queue.put((timestamp, level, message))
        except Exception:
            self.handleError(record)  # Handle any unexpected errors during logging

#=====================================================================================================================================================#
#== Define a function to process the log queue and periodically clear the DataFrame ==#

def process_log_queue(dataframe, queue, clear_interval=10):
    """
    Continuously processes the log queue, adding entries to the DataFrame
    and clearing the DataFrame at specified intervals.
    """
    while True:
        # Process each log entry from the queue
        while not queue.empty():
            timestamp, level, message = queue.get()
            dataframe.loc[len(dataframe)] = [timestamp, level, message]

        # Clear DataFrame contents after the specified interval
        if len(dataframe) > 0:
            dataframe.drop(dataframe.index, inplace=True)  # Clear DataFrame rows

        # Sleep for the clear interval duration before the next check
        time.sleep(clear_interval)

#=====================================================================================================================================================#
#== Set up logging to console, file, and DataFrame ==#

def setup_logging(log_file='app_log.log', log_level=logging.INFO):
    """
    Configure logging to send output to a file, console, and DataFrame.
    Returns a logger instance.
    """
    # Clear any pre-existing handlers to avoid duplicate logs
    logging.root.handlers.clear()

    # Create a logger instance and set the logging level
    logger = logging.getLogger()
    logger.setLevel(log_level)

    # Queue to hold log messages temporarily before processing
    log_queue = Queue()

    # Define handlers to output logs to various destinations
    handlers = [
        RotatingFileHandler(log_file, maxBytes=10485760, backupCount=5),  # File handler with 10 MB file rotation
        logging.StreamHandler(),                                         # Console output handler
        DataFrameLoggingHandler(log_df, log_queue)                       # Custom handler for DataFrame logging
    ]
    
    # Set each handler's level and format, then add to the logger
    for handler in handlers:
        handler.setLevel(log_level)
        handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logger.addHandler(handler)

    # Start a background thread to process the log queue continuously
    log_thread = Thread(target=process_log_queue, args=(log_df, log_queue), daemon=True)
    log_thread.start()

    return logger

#=====================================================================================================================================================#
#== Example Usage of Logger Setup ==#

# Initialize logging configuration
logger = setup_logging(log_file='app_log.log', log_level=logging.INFO)
logger.info("Logging initialized and set up successfully.")

#=====================================================================================================================================================#
#== Function to Display a Preview of the Log DataFrame ==#

def display_log_df(log_df, rows=5, exclude_column=None):
    """
    Display a preview of the log DataFrame, allowing an optional exclusion of a specified column.
    """
    logging.info(f"Viewing the log: {display_log_df}")
    # Exclude specified column from view if present
    if exclude_column in log_df.columns:
        log_df = log_df.drop(columns=exclude_column)
    return log_df.head(rows)

#=====================================================================================================================================================#
#== Example Preview of Logs in DataFrame ==#

# Display the top 20 rows of the log DataFrame
preview_log = display_log_df(log_df, rows=20)
print(preview_log)
