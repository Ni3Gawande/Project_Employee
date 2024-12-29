# secure_config/config_loader.py or project root

from dotenv import load_dotenv
import os

# Load the .env file from the specific path (adjust the path based on where your .env file is located)
load_dotenv(r'C:\Users\Anshu\Desktop\folder\ETL\ETLFramework2\.env')

# Access the log file location from the .env file
log_file_location = os.getenv('log_file_location')

# Print the log file location to verify it's loaded correctly
print(f"Log file location: {log_file_location}")
