from googleapiclient.discovery import build
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
import pandas as pd
import json
from pathlib import Path
import re
import logging
from src.config.settings import TARGET_CHANNEL_IDS
from src.gcp.storage import GCPStorageManager, BigQueryManager, CHANNEL_SCHEMA, VIDEO_SCHEMA

class YouTubeKidsExtractor:
    def __init__(self):
        load_dotenv()
        self.youtube = build('youtube', 'v3', 
                           developerKey=os.getenv('YOUTUBE_API_KEY'))      

        self.target_channels = TARGET_CHANNEL_IDS

        # Get GCP config from environment
        project_id = os.getenv('GCP_PROJECT_ID')
        bucket_name = os.getenv('GCP_BUCKET_NAME')
        dataset_id = os.getenv('GCP_DATASET_ID')

        # Initialize GCP clients
        self.storage_manager = GCPStorageManager(bucket_name, project_id)
        self.bq_manager = BigQueryManager(project_id, dataset_id)

        # Ensure BigQuery resources exist
        self.bq_manager.create_dataset_if_not_exists()
        self.bq_manager.create_table_if_not_exists('channels', CHANNEL_SCHEMA)
        self.bq_manager.create_table_if_not_exists('videos', VIDEO_SCHEMA)

    def clean_text(self, text):
        """Clean unicode characters and HTML entities from text"""
        text = text.encode('ascii', 'ignore').decode('ascii')
        text = text.replace('&amp;', '&')
        text = text.replace('&#39;', "'")
        return text.strip()

    def get_channel_details(self, channel_id):
        """Get detailed channel metrics and store in GCP"""
        try:
            request = self.youtube.channels().list(
                part="snippet,statistics,brandingSettings",
                id=channel_id
            )
            response = request.execute()
            
            if 'items' in response and response['items']:
                channel = response['items'][0]
                
                channel_data = {
                    'channel_id': channel_id,
                    'channel_name': channel['snippet']['title'],
                    'channel_url': f"https://www.youtube.com/channel/{channel_id}",
                    'country': channel['snippet'].get('country', 'Unknown'),
                    'joined_date': channel['snippet']['publishedAt'],
                    'subscriber_count': int(channel['statistics']['subscriberCount']),
                    'total_views': int(channel['statistics']['viewCount']),
                    'extracted_at': datetime.now().isoformat(),
                }
            
                # Save to GCS
                gcs_path = self.storage_manager.upload_json(
                    channel_data, 
                    'raw/channels', 
                    'channel_details'
                ) 

                # Convert to DataFrame and load to BigQuery
                df = pd.DataFrame([channel_data])
                df['subscriber_count'] = df['subscriber_count'].astype(int)
                df['total_views'] = df['total_views'].astype(int)
                df['joined_date'] = pd.to_datetime(df['joined_date'])
                df['extracted_at'] = pd.to_datetime(df['extracted_at'])
                
                self.bq_manager.load_dataframe(df, 'channels')

                return gcs_path
        
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
        """Get video details and store in GCP"""
        videos = []
        next_page_token = None
        quota_used = 0
        
        if start_date:
            start_date = datetime.fromisoformat(start_date.replace('Z', '+00:00')).replace(tzinfo=None)
        if end_date:
            end_date = datetime.fromisoformat(end_date.replace('Z', '+00:00')).replace(tzinfo=None)

        try:
            while True:
                # Search for videos
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

                # Get detailed video information
                video_ids = [item['id']['videoId'] for item in search_response['items']]
                video_request = self.youtube.videos().list(
                    part="contentDetails,statistics",
                    id=','.join(video_ids)
                )
                video_response = video_request.execute()
                quota_used += 1

                # Process video data
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
                        break

                next_page_token = search_response.get('nextPageToken')
                if not next_page_token or (max_results and len(videos) >= max_results):
                    break

            # Save to GCS
            gcs_path = self.storage_manager.upload_json(
                videos, 
                'raw/videos', 
                'video_details'
            )
            
            # Load to BigQuery
            if videos:
                df = pd.DataFrame(videos)
                df['view_count'] = df['view_count'].astype(int)
                df['duration_seconds'] = df['duration_seconds'].astype(int)
                df['upload_datetime'] = pd.to_datetime(df['upload_datetime'])
                df['extracted_at'] = pd.to_datetime(df['extracted_at'])
                self.bq_manager.load_dataframe(df, 'videos')
            
            return gcs_path, quota_used
        
        except Exception as e:
            print(f"Error getting video details: {str(e)}")
            return None, quota_used