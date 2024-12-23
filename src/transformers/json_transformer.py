from pathlib import Path
import json
import pandas as pd
from datetime import datetime
import glob

class YouTubeDataTransformer:
    def __init__(self, raw_data_path: str = "data/raw", processed_data_path: str = "data/processed"):
        self.raw_data_path = Path(raw_data_path)
        self.processed_data_path = Path(processed_data_path)
        self.processed_data_path.mkdir(parents=True, exist_ok=True)
        
    def get_latest_files(self):
        """Get the latest channel and video JSON files"""
        channel_files = glob.glob(str(self.raw_data_path / "channel_details_*.json"))
        video_files = glob.glob(str(self.raw_data_path / "video_details_*.json"))
        
        # Get most recent files based on timestamp in filename
        latest_channel = max(channel_files, key=lambda x: x.split('_')[-1]) if channel_files else None
        latest_video = max(video_files, key=lambda x: x.split('_')[-1]) if video_files else None
        
        return latest_channel, latest_video
    
    def load_raw_data(self):
        """Load the latest JSON files into pandas DataFrames"""
        channel_file, video_file = self.get_latest_files()
        
        with open(channel_file, 'r') as f:
            channel_data = json.load(f)
        
        with open(video_file, 'r') as f:
            video_data = json.load(f)
            
        # Convert to DataFrames
        channel_df = pd.DataFrame([channel_data])
        video_df = pd.DataFrame(video_data)
        
        return channel_df, video_df
    
    def create_dim_channel(self, channel_df):
        """Create channel dimension table"""
        dim_channel = channel_df[[
            'channel_id',
            'channel_name',
            'channel_url',
            'country',
            'joined_date',
            'subscriber_count',
            'total_views'
        ]].copy()
        
        return dim_channel
    
    def create_dim_video(self, video_df):
        """Create video dimension table"""
        dim_video = video_df[[
            'video_id',
            'channel_id',
            'title',
            'url',
            'duration_seconds',
            'upload_datetime'
        ]].copy()
        
        # Add derived attributes
        dim_video['upload_date'] = pd.to_datetime(dim_video['upload_datetime']).dt.date
        dim_video['upload_hour'] = pd.to_datetime(dim_video['upload_datetime']).dt.hour
        dim_video['upload_day_of_week'] = pd.to_datetime(dim_video['upload_datetime']).dt.day_name()
        
        return dim_video
    
    def create_fact_video_metrics(self, video_df):
        """Create video metrics fact table"""
        fact_video_metrics = video_df[[
            'video_id',
            'channel_id',
            'view_count',
            'upload_datetime',
            'extracted_at'
        ]].copy()
        
        # Convert timestamps
        fact_video_metrics['upload_datetime'] = pd.to_datetime(fact_video_metrics['upload_datetime'])
        fact_video_metrics['extracted_at'] = pd.to_datetime(fact_video_metrics['extracted_at'])
        
        return fact_video_metrics
    
    def save_transformed_data(self, dim_channel, dim_video, fact_video_metrics):
        """Save transformed tables"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Save as parquet files
        dim_channel.to_parquet(
            self.processed_data_path / f"dim_channel_{timestamp}.parquet",
            index=False
        )
        
        dim_video.to_parquet(
            self.processed_data_path / f"dim_video_{timestamp}.parquet",
            index=False
        )
        
        fact_video_metrics.to_parquet(
            self.processed_data_path / f"fact_video_metrics_{timestamp}.parquet",
            index=False
        )
    
    def transform(self):
        """Execute full transformation process"""
        print("Starting transformation process...")
        
        # Load raw data
        channel_df, video_df = self.load_raw_data()
        
        # Create dimensional model
        dim_channel = self.create_dim_channel(channel_df)
        dim_video = self.create_dim_video(video_df)
        fact_video_metrics = self.create_fact_video_metrics(video_df)
        
        # Save transformed data
        self.save_transformed_data(dim_channel, dim_video, fact_video_metrics)
        
        print("Transformation complete!")
        return dim_channel, dim_video, fact_video_metrics

if __name__ == "__main__":
    transformer = YouTubeDataTransformer()
    dim_channel, dim_video, fact_video_metrics = transformer.transform()