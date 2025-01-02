# API Configuration
YOUTUBE_API_BASE_URL = "https://www.googleapis.com/youtube/v3"
YOUTUBE_API_QUOTA_LIMIT = 10000  # Daily quota limit

# Channel Configuration
TARGET_CHANNEL_IDS = {
    'Cocomelon': 'UCbCmjCuTUZos6Inko4u57UQ',
    'Baby Shark': 'UCcdwLMPsaU2ezNSJU1nFoBQ',
    'Super Simple Songs': 'UCLsooMJoIpl_7ux2jvdPB-Q',
    'Blippi': 'UC5PYHgAzJ1wLEidB58SK6Xw'
    
}

# API Request Configuration
MAX_RESULTS_PER_REQUEST = 50  # YouTube API max results per page

# Storage Configuration
RAW_DATA_PATH = "data/raw"  # For GCS uploads

# BigQuery Configuration
BQ_TABLES = {
    'channels': 'channels',
    'videos': 'videos'
}

# Logging Configuration
LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"