from pathlib import Path
import json
import pandas as pd
from datetime import datetime
from src.transformers.data_validator import YouTubeDataValidator

class YouTubeDataTransformer:
    def __init__(self, raw_data_path: str = "/opt/airflow/data/raw", processed_data_path: str = "/opt/airflow/data/processed"):
        self.raw_data_path = Path(raw_data_path)
        self.processed_data_path = Path(processed_data_path)
        self.processed_data_path.mkdir(parents=True, exist_ok=True)
    
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
    
    def transform(self, channel_file: str, video_file: str):
        """Execute full transformation process"""
        validator = YouTubeDataValidator()

        print("Starting transformation process...")

        try:
            # Load raw data from specified files
            with open(channel_file, 'r') as f:
                channel_data = json.load(f)
            
            with open(video_file, 'r') as f:
                video_data = json.load(f)
            
            # Convert to DataFrames
            channel_df = pd.DataFrame([channel_data])
            video_df = pd.DataFrame(video_data)
        
            # Validate
            if not validator.validate_channel_data(channel_df):
                raise ValueError("Basic channel validation failed")
            
            if not validator.validate_video_data(video_df):
                raise ValueError("Basic video validation failed")
            
            # Transform the data into dimensional models
            dim_channel = self.create_dim_channel(channel_df)
            dim_video = self.create_dim_video(video_df)
            fact_video_metrics = self.create_fact_video_metrics(video_df)
            
            # Save transformed data
            self.save_transformed_data(dim_channel, dim_video, fact_video_metrics)
            
            print("Transformation complete!")
            return dim_channel, dim_video, fact_video_metrics
        
        except Exception as e:
            print(f"Transform error: {str(e)}")
            raise