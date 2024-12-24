from googleapiclient.discovery import build
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
import pandas as pd
import json
from pathlib import Path
import re
from src.config.settings import TARGET_CHANNELS

class YouTubeKidsExtractor:
    def __init__(self):
        load_dotenv()
        self.youtube = build('youtube', 'v3', 
                           developerKey=os.getenv('YOUTUBE_API_KEY'))
        self.target_channels = TARGET_CHANNELS

    def clean_text(self, text):
        """Clean unicode characters and HTML entities from text"""
        text = text.encode('ascii', 'ignore').decode('ascii')
        text = text.replace('&amp;', '&')
        text = text.replace('&#39;', "'")
        return text.strip()
        
    def ensure_data_directories(self):
        """Create data directories if they don't exist"""
        Path("data/raw").mkdir(parents=True, exist_ok=True)
        Path("data/processed").mkdir(parents=True, exist_ok=True)

    def get_channel_details(self, channel_id):
        """Get detailed channel metrics"""
        try:
            request = self.youtube.channels().list(
                part="snippet,statistics,brandingSettings",
                id=channel_id
            )
            response = request.execute()
            
            if 'items' in response and response['items']:
                channel = response['items'][0]
                
                return {
                    'channel_id': channel_id,
                    'channel_name': channel['snippet']['title'],
                    'channel_url': f"https://www.youtube.com/channel/{channel_id}",
                    'country': channel['snippet'].get('country', 'Unknown'),
                    'joined_date': channel['snippet']['publishedAt'],
                    'subscriber_count': int(channel['statistics']['subscriberCount']),
                    'total_views': int(channel['statistics']['viewCount']),
                    'extracted_at': datetime.now().isoformat(),
                }
        except Exception as e:
            print(f"Error getting channel details: {str(e)}")
            return None

    def parse_duration(self, duration):
        """Convert YouTube duration (PT1H2M10S) to seconds"""
        match = re.match(r'PT((?P<hours>\d+)H)?((?P<minutes>\d+)M)?((?P<seconds>\d+)S)?', duration)
        if not match:
            return 0
        
        parts = {k: int(v) for k, v in match.groupdict().items() if v}
        return parts.get('hours', 0) * 3600 + parts.get('minutes', 0) * 60 + parts.get('seconds', 0)

    def get_video_details(self, channel_id, start_date=None, end_date=None, max_results=None):
        videos = []
        next_page_token = None
        quota_used = 0
        
        if start_date:
            start_date = datetime.fromisoformat(start_date.replace('Z', '+00:00')).replace(tzinfo=None)
        if end_date:
            end_date = datetime.fromisoformat(end_date.replace('Z', '+00:00')).replace(tzinfo=None)

        try:
            while True:
                request = self.youtube.search().list(
                    part="snippet",
                    channelId=channel_id,
                    maxResults=50,
                    order="date",
                    type="video",
                    pageToken=next_page_token,
                    publishedAfter=start_date.isoformat() + 'Z' if start_date else None,
                    publishedBefore=end_date.isoformat() + 'Z' if end_date else None
                )
                search_response = request.execute()
                quota_used += 100
                
                if not search_response.get('items'):
                    break

                video_ids = [item['id']['videoId'] for item in search_response['items']]
                
                video_request = self.youtube.videos().list(
                    part="contentDetails,statistics",
                    id=','.join(video_ids)
                )
                video_response = video_request.execute()
                quota_used += 1

                for search_item, video_item in zip(search_response['items'], video_response['items']):
                    upload_datetime = datetime.fromisoformat(
                        search_item['snippet']['publishedAt'].replace('Z', '+00:00')
                    ).replace(tzinfo=None)
                    
                    duration = video_item['contentDetails']['duration']
                    duration_seconds = self.parse_duration(duration)
                    
                    if duration_seconds < 60:
                        continue
                    
                    videos.append({
                        'video_id': search_item['id']['videoId'],
                        'channel_id': channel_id,
                        'title': self.clean_text(search_item['snippet']['title']),
                        'url': f"https://www.youtube.com/watch?v={search_item['id']['videoId']}",
                        'duration_seconds': duration_seconds,
                        'view_count': int(video_item['statistics']['viewCount']),
                        'upload_datetime': upload_datetime.isoformat(),
                        'extracted_at': datetime.now().isoformat()
                    })
                    
                    if max_results and len(videos) >= max_results:
                        return videos, quota_used

                next_page_token = search_response.get('nextPageToken')
                if not next_page_token:
                    break

            return videos, quota_used
        except Exception as e:
            print(f"Error getting video details: {str(e)}")
            return None, quota_used

    def save_data(self, data, filename):
        """Save data to JSON file with timestamp"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filepath = Path(f"data/raw/{filename}_{timestamp}.json")
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        print(f"Data saved to {filepath}")
        return filepath