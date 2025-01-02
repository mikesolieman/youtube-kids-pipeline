from google.cloud import storage
from google.cloud import bigquery
from pathlib import Path
import json
import pandas as pd
from datetime import datetime
import logging
from src.config.settings import BQ_TABLES, RAW_DATA_PATH

class GCPStorageManager:
    def __init__(self, bucket_name: str, project_id: str):
        self.storage_client = storage.Client(project=project_id)
        self.bucket_name = bucket_name
        self.bucket = self.storage_client.bucket(bucket_name)
        
    def upload_json(self, data: dict, prefix: str, filename: str) -> str:
        """Upload JSON data to GCS bucket"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        blob_name = f"{prefix}/{filename}_{timestamp}.json"
        blob = self.bucket.blob(blob_name)
        
        blob.upload_from_string(
            json.dumps(data, indent=2),
            content_type='application/json'
        )
        
        return f"gs://{self.bucket_name}/{blob_name}"
    
    def list_latest_files(self, prefix: str, pattern: str) -> str:
        """Get latest file matching pattern in prefix"""
        blobs = self.storage_client.list_blobs(
            self.bucket_name, 
            prefix=prefix
        )
        matching_blobs = [
            blob for blob in blobs 
            if blob.name.startswith(prefix) and pattern in blob.name
        ]
        if not matching_blobs:
            return None
            
        return max(matching_blobs, key=lambda x: x.name).name

class BigQueryManager:
    def __init__(self, project_id: str, dataset_id: str):
        self.client = bigquery.Client(project=project_id)
        self.dataset_id = dataset_id
        self.project_id = project_id
        
    def create_dataset_if_not_exists(self):
        """Create BigQuery dataset if it doesn't exist"""
        dataset_ref = self.client.dataset(self.dataset_id)
        try:
            self.client.get_dataset(dataset_ref)
        except Exception:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "US"
            self.client.create_dataset(dataset)
    
    def create_table_if_not_exists(self, table_id: str, schema: list):
        """Create BigQuery table if it doesn't exist"""
        table_ref = f"{self.project_id}.{self.dataset_id}.{BQ_TABLES[table_id]}"
        try:
            self.client.get_table(table_ref)
        except Exception:
            table = bigquery.Table(table_ref, schema=schema)
            self.client.create_table(table)
    
    def load_dataframe(self, df: pd.DataFrame, table_id: str, 
                      write_disposition: str = 'WRITE_APPEND'): # This keeps all versions of data
        """Load pandas DataFrame to BigQuery table"""
        try:
            table_ref = f"{self.project_id}.{self.dataset_id}.{BQ_TABLES[table_id]}"
            print(f"Loading {df.shape[0]} rows to {table_ref}")

            job_config = bigquery.LoadJobConfig(
                write_disposition=write_disposition
            )

            load_job = self.client.load_table_from_dataframe(
                df, table_ref, job_config=job_config
            )
            load_job.result()

            print(f"Successfully loaded {df.shape[0]} rows to {table_ref}")

        except Exception as e:
            print(f"Error loading data to BigQuery: {str(e)}")
            raise

# Schema definitions
CHANNEL_SCHEMA = [
    bigquery.SchemaField("channel_id", "STRING"),
    bigquery.SchemaField("channel_name", "STRING"),
    bigquery.SchemaField("channel_url", "STRING"),
    bigquery.SchemaField("country", "STRING"),
    bigquery.SchemaField("joined_date", "TIMESTAMP"),
    bigquery.SchemaField("subscriber_count", "INTEGER"),
    bigquery.SchemaField("total_views", "INTEGER"),
    bigquery.SchemaField("extracted_at", "TIMESTAMP")
]

VIDEO_SCHEMA = [
    bigquery.SchemaField("video_id", "STRING"),
    bigquery.SchemaField("channel_id", "STRING"),
    bigquery.SchemaField("title", "STRING"),
    bigquery.SchemaField("url", "STRING"),
    bigquery.SchemaField("duration_seconds", "INTEGER"),
    bigquery.SchemaField("view_count", "INTEGER"),
    bigquery.SchemaField("upload_datetime", "TIMESTAMP"),
    bigquery.SchemaField("extracted_at", "TIMESTAMP")
]