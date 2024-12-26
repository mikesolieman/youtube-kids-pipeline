FROM python:3.9-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Set PYTHONPATH for custom modules
ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow/src"

# Copy application code (if any)
COPY . .

# Create necessary directories
RUN mkdir -p /app/data

# Expose ports for the webserver
EXPOSE 8888 8080
