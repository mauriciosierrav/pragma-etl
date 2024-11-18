"""Module providing transformation functions for the ETL pipeline."""

import pandas as pd
from config import logger


def add_processed_date(chunk):
    """Add a processed date to the chunk."""
    try:
        chunk["processed_date"] = pd.to_datetime("now")
        return chunk
    except Exception as e:
        logger.error(f"Error while adding processed date: {e}")
        raise


def convert_to_date(chunk):
    """Convert the timestamp column to a datetime object."""
    try:
        chunk["timestamp"] = pd.to_datetime(chunk["timestamp"], format="%m/%d/%Y")
        return chunk
    except Exception as e:
        logger.error(f"Error while converting timestamp to date: {e}")
        raise


def add_date_partition(row):
    """Add year, month and day columns to the row."""
    try:
        row["day"] = row["timestamp"].day
        row["month"] = row["timestamp"].month
        row["year"] = row["timestamp"].year
        return row
    except Exception as e:
        logger.error(f"Error while adding date partition: {e}")
        raise
