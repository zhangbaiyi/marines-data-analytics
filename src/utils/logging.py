import logging
import os
from datetime import datetime

from colorlog import ColoredFormatter

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
