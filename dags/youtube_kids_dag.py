"""
YouTube Kids Daily Pipeline DAG

This DAG runs daily to:
# Load latest channel metadata for all configured channels
# Extract videos uploaded in the last 24 hours
# Track daily metrics including:
    - Channel subscriber counts and views
    - New video uploads
    - View count changes
# Validate data quality:
    - Ensure all channels are processed
    - Verify successful video extraction
    - Check data completeness and freshness

Key features:
- Incremental daily processing (last 24h window)
- API quota management
- Data quality validation
- Full logging and error handling

Channel List:
- Cocomelon
- Baby Shark
- Super Simple Songs
- Blippi

Dependencies:
- YouTube Data API v3
- BigQuery for data storage
- GCS for raw data backup
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
import os
from pathlib import Path
from google.cloud import bigquery

# Import existing modules
from src.extractors.youtube_extractor import YouTubeKidsExtractor
from src.config.settings import TARGET_CHANNEL_IDS

# Configure logging
logger = logging.getLogger(__name__)

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Initialize DAG
dag = DAG(
    'youtube_kids_pipeline',
    default_args=default_args,
    description='Daily batch pipeline for YouTube Kids content',
    schedule_interval='0 0 * * *',  # Run daily at midnight
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['youtube', 'batch_processing'],
)

def extract_channel_data(**context):
    """Extract channel data from Youtube API"""
    try:
        extractor = YouTubeKidsExtractor()
        processed_count = 0
        
        for channel_name, channel_id in TARGET_CHANNEL_IDS.items():
            logger.info(f"Processing channel: {channel_name}")
            result = extractor.get_channel_details(channel_id)
            
            if result:
                processed_count += 1
                logger.info(f"Successfully processed {channel_name}")
            else:
                logger.warning(f"No data extracted for {channel_name}")
                
        if processed_count == 0:
            raise ValueError("No channels were successfully processed")
            
        logger.info(f"Completed channel extraction. Processed {processed_count} channels")
        return True
        
    except Exception as e:
        logger.error(f"Channel extraction failed: {str(e)}")
        raise

def extract_video_data(**context):
    """Extract video data from YouTube API"""
    try:
        extractor = YouTubeKidsExtractor()
        processed_count = 0
        total_quota = 0
        
        # Get videos from last 24 hours
        end_date = datetime.now().isoformat()
        start_date = (datetime.now() - timedelta(days=1)).isoformat()
        
        for channel_name, channel_id in TARGET_CHANNEL_IDS.items():
            logger.info(f"Processing videos for channel: {channel_name}")
            
            result, quota = extractor.get_video_details(
                channel_id,
                start_date=start_date,
                end_date=end_date
            )
            
            if result:
                processed_count += 1
                total_quota += quota
                logger.info(f"Successfully processed videos for {channel_name}")
            else:
                logger.warning(f"No video data extracted for {channel_name}")
                
        logger.info(f"Completed video extraction. Processed {processed_count} channels")
        logger.info(f"Total API quota used: {total_quota}")
        
        if processed_count == 0:
            raise ValueError("No videos were successfully processed")
            
        return True
        
    except Exception as e:
        logger.error(f"Video extraction failed: {str(e)}")
        raise

def verify_data_quality(**context):
    """Verify daily data load quality"""
    try:
        project_id = os.getenv('GCP_PROJECT_ID')
        dataset_id = os.getenv('GCP_DATASET_ID')
        client = bigquery.Client(project=project_id)

        # Check today's channel updates
        channel_query = f"""
            SELECT 
                COUNT(DISTINCT channel_id) as channels_updated,
                COUNT(*) as total_updates
            FROM `{project_id}.{dataset_id}.channels`
            WHERE DATE(extracted_at) = CURRENT_DATE()
        """
        channel_results = list(client.query(channel_query).result())[0]

        # Check today's video updates
        video_query = f"""
            SELECT 
                COUNT(*) as new_videos,
                COUNT(DISTINCT channel_id) as channels_with_videos,
                MIN(DATE(upload_datetime)) as earliest_video,
                MAX(DATE(upload_datetime)) as latest_video
            FROM `{project_id}.{dataset_id}.videos`
            WHERE DATE(extracted_at) = CURRENT_DATE()
        """
        video_results = list(client.query(video_query).result())[0]

        # Log validation results
        logger.info("Daily Load Statistics:")
        logger.info(f"Channel Updates:")
        logger.info(f"- Channels updated today: {channel_results.channels_updated}")
        logger.info(f"- Total channel updates: {channel_results.total_updates}")
        logger.info(f"Video Updates:")
        logger.info(f"- New videos loaded today: {video_results.new_videos}")
        logger.info(f"- Channels with new videos: {video_results.channels_with_videos}")
        if video_results.new_videos > 0:
            logger.info(f"- Video date range: {video_results.earliest_video} to {video_results.latest_video}")

        # Validation checks
        if channel_results.channels_updated < len(TARGET_CHANNEL_IDS):
            raise ValueError(
                f"Missing channel updates. Expected {len(TARGET_CHANNEL_IDS)} channels, "
                f"got {channel_results.channels_updated}"
            )
        
        if video_results.new_videos == 0:
            logger.warning("No new videos found in last 24 hours")

        return True

    except Exception as e:
        logger.error(f"Daily data validation failed: {str(e)}")
        raise

# Define tasks
extract_channels_task = PythonOperator(
    task_id='extract_channel_data',
    python_callable=extract_channel_data,
    provide_context=True,
    dag=dag,
)

extract_videos_task = PythonOperator(
    task_id='extract_video_data',
    python_callable=extract_video_data,
    provide_context=True,
    dag=dag,
)

validate_data_task = PythonOperator(
    task_id='verify_data_quality',
    python_callable=verify_data_quality,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
extract_channels_task >> extract_videos_task >> validate_data_task