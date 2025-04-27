import logging
import os
from collections import deque
from datetime import datetime

import streamlit as st
from colorlog import ColoredFormatter
from streamlit.delta_generator import DeltaGenerator  # To type hint the container

CURRENT_WORKING_DIRECTORY = os.getcwd()
LOG_DIR = os.path.join(CURRENT_WORKING_DIRECTORY, "logs")
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

CURRENT_TIMESTAMP = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
LOG_FILE_NAME = f"logs/simple_ml_{CURRENT_TIMESTAMP}.log"
logging_stream_handler = logging.StreamHandler()
logging_file_handler = logging.FileHandler(LOG_FILE_NAME)

# Define the colored formatter for stdout
colorlog_formatter = ColoredFormatter(
    fmt="%(log_color)s[%(asctime)s] LN#%(lineno)-4d: %(levelname)-8s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    log_colors={
        "DEBUG": "cyan",  # Matches Pino's debug color
        "INFO": "green",  # Matches Pino's default info color
        "WARNING": "yellow",  # Matches Pino's warning color
        "ERROR": "red",  # Matches Pino's error color
        "CRITICAL": "bold_red",  # Matches Pino's fatal/critical color
    },
    secondary_log_colors={},
    style="%",
)

# Define the standard formatter for file logging (no colors)
file_formatter = logging.Formatter(
    "[%(asctime)s] LN#%(lineno)-4d: %(levelname)-8s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Attach the handler
logging_stream_handler.setFormatter(colorlog_formatter)
logging_file_handler.setFormatter(file_formatter)

# Configure the logger
LOGGER = logging.getLogger("pino_logger_with_file")
LOGGER.addHandler(logging_stream_handler)
LOGGER.addHandler(logging_file_handler)
LOGGER.setLevel(logging.DEBUG)


# Define the custom handler
class StreamlitLogHandler(logging.Handler):
    """
    A logging handler that emits the latest N logs to a Streamlit container
    using a monospace font.
    """

    def __init__(self, container: DeltaGenerator, max_messages: int = 5, level=logging.NOTSET):
        super().__init__(level=level)
        self.container = container
        # Use deque for efficient fixed-size buffer (automatically discards oldest)
        self.log_buffer = deque(maxlen=max_messages)
        # Create a placeholder within the container. We'll update this placeholder.
        self.log_placeholder = self.container.empty()

    def emit(self, record: logging.LogRecord):
        """
        Adds the log record to the buffer and updates the Streamlit placeholder.
        """
        try:
            # Format the log record into a string
            msg = self.format(record)
            # Add the message to our fixed-size buffer
            self.log_buffer.append(msg)

            # Prepare the text to display (join all messages in the buffer)
            log_content = "\n".join(self.log_buffer)

            # Update the placeholder with the log content using st.code
            # st.code uses monospace font by default. language=None prevents syntax highlighting.
            self.log_placeholder.code(log_content, language=None)

        except RecursionError:
            raise
        except Exception:
            self.handleError(record)

    def clear(self):
        """Clears the log display area."""
        self.log_buffer.clear()
        self.log_placeholder.empty()  # Clear the content of the placeholder
