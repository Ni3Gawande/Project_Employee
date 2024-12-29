# config_loader.py

from dotenv import load_dotenv
import os
from pathlib import Path

# Load the .env file (relative path)
dotenv_path = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(dotenv_path)

# Access the log file location from the .env file
log_file_location = os.getenv('log_file_location')

# Print the log file location to verify it's loaded correctly
print(f"Log file location: {log_file_location}")
