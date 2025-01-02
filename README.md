# YouTube Kids Content Analytics Pipeline

A data pipeline I built at Disney Streaming to optimize our content strategy. The pipeline enables analysis of key metrics like video duration, upload frequency patterns, and optimal posting schedules across top kids' channels. The insights reveal how successful channels balance content length with posting frequency, helping inform programming strategy. This version demonstrates the same techniques using public YouTube data.

## Core Features
- Automated data pipeline with Airflow orchestration
- Data warehouse integration using BigQuery and Cloud Storage
- Daily metric tracking with historical data support
- Containerized development environment with Docker

## Requirements
- Docker and Docker Compose
- Python 3.9+
- YouTube Data API credentials
- Google Cloud Platform account & credentials

## Getting Started

1. Clone the repository
2. Create `.env` file with required credentials:
   - `YOUTUBE_API_KEY`
   - `GCP_PROJECT_ID`
   - `GCP_BUCKET_NAME`
   - `GCP_DATASET_ID`
   - `AIRFLOW_ADMIN_USERNAME`
   - `AIRFLOW_ADMIN_PASSWORD`
3. Place GCP service account key in project root
4. Run `docker-compose up`

Services will be available at:
- Jupyter Lab: http://localhost:8889
- Airflow: http://localhost:8084

## Project Structure
```
youtube-kids-pipeline/
├── dags/                # Airflow DAGs (daily & backfill)
├── src/
│   ├── extractors/     # YouTube API integration
│   ├── gcp/            # Cloud storage handlers
│   └── config/         # Settings and schemas
├── data/               # Local development data
└── tests/              # Test modules
```