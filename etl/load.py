"""Module providing load functions for the ETL pipeline."""

import pandas as pd
from config import logger
from utils import MySQLDatabase


def load_row(db: MySQLDatabase, table_name: str, row: pd.Series):
    """Load data into a MySQL database."""
    try:
        values_to_insert = (
            row.timestamp if pd.notna(row.timestamp) else None,
            row.day if pd.notna(row.day) else None,
            row.month if pd.notna(row.month) else None,
            row.year if pd.notna(row.year) else None,
            row.price if pd.notna(row.price) else None,
            row.user_id if pd.notna(row.user_id) else None,
            row.processed_date if pd.notna(row.processed_date) else None,
        )

        db.insert(
            f"INSERT INTO {table_name}"
            + "(timestamp, day, month, year, price, user_id, processed_date) VALUES (%s, %s, %s, %s, %s, %s, %s)",
            values_to_insert,
        )
    except Exception as e:
        logger.fatal(f"Error while loading row '{row}' into MySQL: {e}")
        raise
