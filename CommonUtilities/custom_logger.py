import logging
import os
from dotenv import load_dotenv

load_dotenv()


class LogGen:
    def logger(self):
        # Define the log directory and file
        LOG_DIR = os.getenv("log_file_location")
        os.makedirs(LOG_DIR, exist_ok=True)  # Ensure log directory exists
        LOG_FILE = os.path.join(LOG_DIR, 'etlprocess.log')

        # Create a logger
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)  # Set the logging level

        # File handler for logging to a file
        file_handler = logging.FileHandler(LOG_FILE)
        file_handler.setLevel(logging.INFO)  # File log level
        file_formatter = logging.Formatter('%(asctime)s  - %(levelname)s - %(message)s')
        file_handler.setFormatter(file_formatter)

        # Add the file handler to the logger
        if not logger.handlers:  # Avoid adding handlers multiple times
            logger.addHandler(file_handler)

        # Log file path info (for debugging purposes)
        # print(f"Logging to file: {LOG_FILE}")
        return logger
