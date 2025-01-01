# YouTube Kids Content Analytics Pipeline

A data pipeline I built at Disney Streaming to optimize our content strategy. The pipeline analyzes key metrics like video duration, upload frequency patterns, and optimal posting schedules across top kids' channels. These insights revealed how successful channels balanced content length with posting frequency, helping inform programming strategy. This version demonstrates the same techniques using public YouTube data.

## Core Features
- Automated ETL pipeline with Airflow orchestration and quota management
- Dimensional data model optimized for content performance analysis
- Containerized environment with Docker, Jupyter, and Airflow

## Requirements
- Docker and Docker Compose
- Python 3.9+
- YouTube Data API credentials

## Getting Started

1. Clone the repository
2. Create `.env` file with required credentials:
   - `YOUTUBE_API_KEY`
   - `AIRFLOW_ADMIN_USERNAME`
   - `AIRFLOW_ADMIN_PASSWORD`
3. Run `docker-compose up`

Services will be available at:
- Jupyter Lab: http://localhost:8889
- Airflow: http://localhost:8084

## Project Structure
```
youtube-kids-pipeline/
├── dags/                # Airflow DAGs
├── data/               
│   ├── raw/            # JSON from API
│   └── processed/      # Transformed Parquet
├── src/
│   ├── extractors/     # YouTube API handlers
│   ├── transformers/   # Data transformation
│   └── config/         # Settings
└── tests/              # Test modules
```