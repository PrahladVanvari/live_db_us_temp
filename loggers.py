import logging
import os
from datetime import datetime

# Ensure log directory exists
LOG_DIR = 'logs'
os.makedirs(LOG_DIR, exist_ok=True)

# Generate a timestamped filename
# timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
timestamp = datetime.now().strftime('%Y%m%d')
# Formatter for all loggers
_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

LOGGING_LEVEL = logging.INFO
# LOGGING_LEVEL = logging.DEBUG

def _create_logger(name, level=LOGGING_LEVEL):
    """Creates and returns a logger with a given name and level."""
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Prevent adding multiple handlers to the same logger
    if not logger.handlers:
        # File handler
        log_file_path = os.path.join(LOG_DIR, f'{name}_{timestamp}.log')
        file_handler = logging.FileHandler(log_file_path)
        file_handler.setFormatter(_formatter)
        logger.addHandler(file_handler)

    return logger

# Pre-declared loggers
polygon_logger = _create_logger('polygon')
options_logger = _create_logger('options')
index_logger = _create_logger('index')
queue_logger = _create_logger('consumer_queue')
resampler_logger = _create_logger('resampler')