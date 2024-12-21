import pandas as pd
from CommonUtilities.custom_logger import logger
import os

def check_file_exists(file_path):
    if os.path.exists(file_path):
        return True
    else:
        raise ValueError(f"File not found at: {file_path}")
