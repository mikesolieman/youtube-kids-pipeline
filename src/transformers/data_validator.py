# Only essential checks: Data presence, Required fields, Basic metric validation

import pandas as pd
from datetime import datetime
import logging

class YouTubeDataValidator:
    def __init__(self):
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    def validate_channel_data(self, df: pd.DataFrame) -> bool:
        """Basic validation for channel dimension"""
        try:
            # Essential checks only
            if df.empty:
                self.logger.error("Channel data is empty")
                return False

            # Required fields present
            required_fields = ['channel_id', 'channel_name']
            if not all(field in df.columns for field in required_fields):
                self.logger.error(f"Missing required fields in channel data")
                return False

            # Basic metric validation
            if (df['subscriber_count'] < 0).any() or (df['total_views'] < 0).any():
                self.logger.error("Found negative values in metrics")
                return False

            return True

        except Exception as e:
            self.logger.error(f"Channel validation error: {str(e)}")
            return False

    def validate_video_data(self, df: pd.DataFrame) -> bool:
        """Basic validation for video dimension"""
        try:
            if df.empty:
                self.logger.error("Video data is empty")
                return False

            # Required fields present
            required_fields = ['video_id', 'title', 'upload_datetime']
            if not all(field in df.columns for field in required_fields):
                self.logger.error(f"Missing required fields in video data")
                return False

            # Basic duration check
            if (df['duration_seconds'] <= 0).any():
                self.logger.error("Invalid video duration found")
                return False

            return True

        except Exception as e:
            self.logger.error(f"Video validation error: {str(e)}")
            return False
