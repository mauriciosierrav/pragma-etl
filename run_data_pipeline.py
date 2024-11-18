"""ETL pipeline to process the data files."""

from logging import Logger
import pandas as pd
from config import logger
from etl import (
    micro_batches_generator,
    add_processed_date,
    convert_to_date,
    add_date_partition,
    load_row,
)
from utils import MySQLDatabase


def run_data_pipeline(
    files,
    db: MySQLDatabase,
    table_name: str,
    chunk_size: int,
    metrics_logger: Logger,
):
    """Pipeline to process the data files."""

    total_rows = 0
    prices = []

    # Process each file individually to avoid memory overload
    for f in files:
        logger.info(f"Processing file '{f}'...")
        micro_batches = micro_batches_generator(file_path=f, chunk_size=chunk_size)
        total_rows_file = 0

        # Process each microbatch and add necessary columns
        for n, chunk in enumerate(micro_batches):
            logger.debug(f"Processing microbatch {n+1}...")
            chunk = add_processed_date(chunk)
            chunk = convert_to_date(chunk)
            chunk = chunk.apply(add_date_partition, axis=1)

            # Process each row in the microbatch and collect price data for statistics calculation
            for row in chunk.itertuples(index=False):
                total_rows += 1
                total_rows_file += 1
                actual_row_price = None if pd.isna(row.price) else row.price
                prices.append(actual_row_price)
                valid_values = [x for x in prices if x is not None]

                # Calculate statistics (sum, average, min, max) for price
                sum_price = sum(valid_values)
                avg_price = sum(valid_values) / len(valid_values)
                min_price = min(valid_values)
                max_price = max(valid_values)

                # Load the current row into the database
                load_row(db=db, table_name=table_name, row=row)

                # Log metrics
                metrics_logger.info(
                    f"""
                    -----------------------------------------
                    ---------- ACTUAL FILE METRICS ----------
                    -----------------------------------------
                    Actual file: '{f}',
                    Microbatch: {n+1},
                    Total rows processed: {total_rows_file},
                    Actual row price: {actual_row_price},

                    -----------------------------------------
                    -------- ACTUAL PIPELINE METRICS --------
                    -----------------------------------------
                    Total rows processed: {total_rows},
                    Sum price: {sum_price},
                    Average price: {avg_price}
                    Min price: {min_price}
                    Max price: {max_price}
                    """
                )
            logger.debug(f"Microbatch {n+1} processed successfully")

        logger.info(f"File '{f}' processed successfully")
