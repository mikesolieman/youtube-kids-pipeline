# Test notebook content
import os
from google.oauth2 import service_account
from googleapiclient.discovery import build
import pandas as pd

# Test environment setup
print("Python packages imported successfully!")

# Test YouTube API connection
try:
    youtube = build('youtube', 'v3', 
                   developerKey=os.getenv('YOUTUBE_API_KEY'))
    request = youtube.channels().list(
        part="snippet,statistics",
        id="UCbCmjCuTUZos6Inko4u57UQ"  # Cocomelon channel ID
    )
    response = request.execute()
    print("YouTube API connection successful!")
    print(f"Channel Name: {response['items'][0]['snippet']['title']}")
    print(f"Subscriber Count: {response['items'][0]['statistics']['subscriberCount']}")
except Exception as e:
    print(f"Error connecting to YouTube API: {str(e)}")
