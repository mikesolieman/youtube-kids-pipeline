from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
from pathlib import Path

# Import your existing modules
from src.extractors.youtube_extractor import YouTubeKidsExtractor
from src.transformers.json_transformer import YouTubeDataTransformer
from src.config.settings import START_DATE, END_DATE, TARGET_CHANNEL_IDS

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
    description='Pipeline for YouTube Kids content analysis',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['youtube', 'content_analysis'],
)

# Initialize extractors 
extractor = YouTubeKidsExtractor()
extractor.ensure_data_directories()
transformer = YouTubeDataTransformer()


def extract_channel_details(**context):
    """Extract channel details from Youtube API"""
    #Specify channels to extract
    channel_id = TARGET_CHANNEL_IDS['Cocomelon']
    channel_data = extractor.get_channel_details(channel_id)
    if channel_data:
        output_path = extractor.save_data(channel_data, "channel_details")
        logger.info(f"Extracted channel data saved to {output_path}")
        return str(output_path)

def extract_video_details(**context):
    """Extract video data from YouTube API"""
    #Specify videos to extract
    channel_id = TARGET_CHANNEL_IDS['Cocomelon']
    video_data, quota_used = extractor.get_video_details(
    channel_id,
    start_date=START_DATE,
    end_date=END_DATE
    )
    if video_data:
        output_path = extractor.save_data(video_data, "video_details")
        logger.info(f"Extracted video data saved to {output_path}")
        return str(output_path)

def transform_data(**context):
    """Transform raw data into dimensional model"""
    try:
        # Get paths of files created by previous tasks
        ti = context['task_instance']
        channel_file = ti.xcom_pull(task_ids='extract_channel_details_task')
        video_file = ti.xcom_pull(task_ids='extract_video_details_task')

        if not channel_file or not video_file:
            raise ValueError("Files from extraction tasks not found")

        # Load and transform raw data
        dim_channel, dim_video, fact_video_metrics = transformer.transform(
            channel_file=channel_file,
            video_file=video_file
        )
        logger.info("Transformation completed!")
        return True
    
    except Exception as e:
        logger.error(f"Transform task failed: {str(e)}")
        raise

# Define tasks
extract_channel_details_task = PythonOperator(
    task_id='extract_channel_details_task',
    python_callable=extract_channel_details,
    provide_context=True,
    dag=dag,
)

extract_videos_task = PythonOperator(
    task_id='extract_video_details_task',
    python_callable=extract_video_details,
    provide_context=True,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
extract_channel_details_task >> extract_videos_task >> transform_task