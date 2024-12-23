from googleapiclient.discovery import build
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
import pandas as pd
from src.config.settings import TARGET_CHANNELS

class YouTubeKidsExtractor:
    def __init__(self):
        load_dotenv()
        self.youtube = build('youtube', 'v3', 
                           developerKey=os.getenv('YOUTUBE_API_KEY'))
        self.target_channels = TARGET_CHANNELS
    
    def get_channel_stats(self, channel_id):
        """Get basic channel statistics"""
        request = self.youtube.channels().list(
            part="statistics,snippet",
            id=channel_id
        )
        response = request.execute()
        
        if 'items' in response:
            stats = response['items'][0]['statistics']
            snippet = response['items'][0]['snippet']
            return {
                'channelName': snippet['title'],
                'subscribers': stats['subscriberCount'],
                'totalViews': stats['viewCount'],
                'totalVideos': stats['videoCount'],
                'channelDescription': snippet['description'],
                'channelId': channel_id,
                'extractDate': datetime.now().strftime('%Y-%m-%d')
            }
        return None

    def get_recent_videos(self, channel_id, max_results=10):
        """Get most recent videos from channel"""
        request = self.youtube.search().list(
            part="snippet",
            channelId=channel_id,
            order="date",
            type="video",
            maxResults=max_results
        )
        response = request.execute()
        
        videos = []
        for item in response.get('items', []):
            video_id = item['id']['videoId']
            video_stats = self.get_video_stats(video_id)
            
            video_data = {
                'videoId': video_id,
                'title': item['snippet']['title'],
                'publishedAt': item['snippet']['publishedAt'],
                'description': item['snippet']['description'],
                'channelId': channel_id,
                'extractDate': datetime.now().strftime('%Y-%m-%d')
            }
            videos.append({**video_data, **video_stats})
            
        return videos
    
    def get_video_stats(self, video_id):
        """Get statistics for a specific video"""
        request = self.youtube.videos().list(
            part="statistics,contentDetails",
            id=video_id
        )
        response = request.execute()
        
        if 'items' in response:
            stats = response['items'][0]['statistics']
            content_details = response['items'][0]['contentDetails']
            return {
                'views': stats.get('viewCount', 0),
                'likes': stats.get('likeCount', 0),
                'comments': stats.get('commentCount', 0),
                'duration': content_details['duration']
            }
        return {}

    def extract_all_channels(self):
        """Extract data for all target channels"""
        channel_stats = []
        all_videos = []
        
        for channel_name, channel_id in self.target_channels.items():
            print(f"Extracting data for {channel_name}...")
            
            # Get channel statistics
            stats = self.get_channel_stats(channel_id)
            if stats:
                channel_stats.append(stats)
            
            # Get recent videos
            videos = self.get_recent_videos(channel_id)
            all_videos.extend(videos)
        
        # Convert to DataFrames
        channels_df = pd.DataFrame(channel_stats)
        videos_df = pd.DataFrame(all_videos)
        
        return channels_df, videos_df

if __name__ == "__main__":
    # Usage example
    extractor = YouTubeKidsExtractor()
    channels_df, videos_df = extractor.extract_all_channels()
    
    # Save to CSV for initial testing
    channels_df.to_csv('channel_stats.csv', index=False)
    videos_df.to_csv('recent_videos.csv', index=False)
    
    print("Data extraction complete!")