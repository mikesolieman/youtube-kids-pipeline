"""
Loads all channel metadata
Gets all videos from 2023 to present
Processes in 3-month chunks to manage API quota
Has quota management (stops at 8000 units)
Is set to manual trigger (schedule_interval=None)
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
import os
from pathlib import Path
from google.cloud import bigquery

from src.extractors.youtube_extractor import YouTubeKidsExtractor
from src.config.settings import TARGET_CHANNEL_IDS

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'youtube_kids_historical_backfill',
    default_args=default_args,
    description='One-time historical data load for YouTube Kids content',
    schedule_interval=None,  # Manual triggers only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['youtube', 'backfill', 'historical'],
)

def extract_historical_channel_data(**context):
    """Extract channel data - same as daily DAG since we want current state"""
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
        
        return True
    
    except Exception as e:
        logger.error(f"Channel extraction failed: {str(e)}")
        raise

def extract_historical_video_data(**context):
    """Extract historical video data with chunked date ranges"""
    try:
        extractor = YouTubeKidsExtractor()
        total_quota = 0
        processed_videos = 0
        
        # Define date ranges for historical load
        end_date = datetime.now()
        start_date = datetime(2023, 1, 1)  # Historical start date
        
        # Process each channel
        for channel_name, channel_id in TARGET_CHANNEL_IDS.items():
            logger.info(f"Processing historical videos for channel: {channel_name}")
            
            # Process in 3-month chunks to manage API quota and memory
            current_start = start_date
            while current_start < end_date:
                current_end = min(
                    current_start + timedelta(days=90),
                    end_date
                )
                
                logger.info(
                    f"Processing chunk: {current_start.date()} to {current_end.date()}"
                )
                
                result, quota = extractor.get_video_details(
                    channel_id,
                    start_date=current_start.isoformat(),
                    end_date=current_end.isoformat()
                )
                
                if result:
                    processed_videos += 1
                    total_quota += quota
                    logger.info(
                        f"Chunk processed successfully. "
                        f"Quota used so far: {total_quota}"
                    )
                else:
                    logger.warning(
                        f"No data for {channel_name} between "
                        f"{current_start.date()} and {current_end.date()}"
                    )
                
                # Move to next chunk
                current_start = current_end
                
                # Optional: Add delay between chunks to manage quota
                if total_quota > 8000:  # 80% of daily quota
                    raise ValueError(
                        f"Quota limit approaching ({total_quota}). "
                        "Consider resuming tomorrow."
                    )
        
        logger.info(f"Historical load complete. Total API quota used: {total_quota}")
        logger.info(f"Total video chunks processed: {processed_videos}")
        
        return True
        
    except Exception as e:
        logger.error(f"Historical video extraction failed: {str(e)}")
        raise

def verify_historical_load(**context):
    """Verify historical data load quality"""
    try:
        project_id = os.getenv('GCP_PROJECT_ID')
        dataset_id = os.getenv('GCP_DATASET_ID')
        client = bigquery.Client(project=project_id)
        
        # Check total video count and distribution
        video_query = f"""
            SELECT 
                COUNT(*) as total_videos,
                MIN(DATE(upload_datetime)) as earliest_date,
                MAX(DATE(upload_datetime)) as latest_date,
                COUNT(DISTINCT channel_id) as channel_count
            FROM `{project_id}.{dataset_id}.videos`
        """
        video_results = list(client.query(video_query).result())[0]
        
        # Check channel coverage
        channel_query = f"""
            SELECT COUNT(DISTINCT channel_id) as channel_count
            FROM `{project_id}.{dataset_id}.channels`
        """
        channel_results = list(client.query(channel_query).result())[0]
        
        # Log results
        logger.info("Historical Load Statistics:")
        logger.info(f"Total Videos: {video_results.total_videos}")
        logger.info(f"Date Range: {video_results.earliest_date} to {video_results.latest_date}")
        logger.info(f"Channels with Videos: {video_results.channel_count}")
        logger.info(f"Channels in Metadata: {channel_results.channel_count}")
        
        # Validation checks
        if channel_results.channel_count < len(TARGET_CHANNEL_IDS):
            raise ValueError(
                f"Missing channel data. Expected {len(TARGET_CHANNEL_IDS)} channels, "
                f"got {channel_results.channel_count}"
            )
            
        if video_results.channel_count < len(TARGET_CHANNEL_IDS):
            raise ValueError(
                f"Missing videos for some channels. Expected {len(TARGET_CHANNEL_IDS)} "
                f"channels with videos, got {video_results.channel_count}"
            )
        
        return True
        
    except Exception as e:
        logger.error(f"Historical load validation failed: {str(e)}")
        raise

# Define tasks
extract_channels_task = PythonOperator(
    task_id='extract_historical_channel_data',
    python_callable=extract_historical_channel_data,
    provide_context=True,
    dag=dag,
)

extract_videos_task = PythonOperator(
    task_id='extract_historical_video_data',
    python_callable=extract_historical_video_data,
    provide_context=True,
    dag=dag,
)

validate_historical_task = PythonOperator(
    task_id='verify_historical_load',
    python_callable=verify_historical_load,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
extract_channels_task >> extract_videos_task >> validate_historical_task