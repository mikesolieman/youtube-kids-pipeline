# src/config/settings.py

# API Configuration
YOUTUBE_API_BASE_URL = "https://www.googleapis.com/youtube/v3"
API_REQUEST_DELAY = 0.1  # seconds between API calls

# Channel Configuration
TARGET_CHANNELS = {
    'Cocomelon': 'UCbCmjCuTUZos6Inko4u57UQ',
    'Baby Shark': 'UCcdwLMPsaU2ezNSJU1nFoBQ',
    'Super Simple Songs': 'UCLsooMJoIpl_7ux2jvdPB-Q',
    'Blippi': 'UC5PYHfAzxe-64vRjWG7yxTA'
}

# Data Collection Settings
MAX_RESULTS_PER_CHANNEL = 50
DATA_COLLECTION_PERIOD_DAYS = 30

# File Paths
RAW_DATA_PATH = "data/raw"
PROCESSED_DATA_PATH = "data/processed"

# Logging Configuration
LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
