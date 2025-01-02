#!/bin/bash

# Activate service account if credentials are provided
if [ -n "$GOOGLE_APPLICATION_CREDENTIALS" ]; then
    if [ -f "$GOOGLE_APPLICATION_CREDENTIALS" ]; then
        gcloud auth activate-service-account --key-file="$GOOGLE_APPLICATION_CREDENTIALS"
        echo "GCP service account activated"
    else
        echo "Warning: GOOGLE_APPLICATION_CREDENTIALS file not found at $GOOGLE_APPLICATION_CREDENTIALS"
    fi
fi

# Execute the provided command
exec "$@"