"""Module providing extraction functions for the ETL pipeline."""

import os
import pandas as pd
from config import logger


def get_data_files(exclude_validation: bool = False, only_validation: bool = False):
    """Generates a list of files from the data folder to be processed."""
    try:
        if exclude_validation and only_validation:
            raise ValueError("Cannot set both 'exclude_validation' and 'only_validation' to True.")

        # Get all csv files in the data folder
        files = sorted([f"./data/{f}" for f in os.listdir("data") if f.endswith(".csv")])

        # Filter files based on the parameters
        if exclude_validation:
            files = [f for f in files if "validation" not in f]
        if only_validation:
            files = [f for f in files if "validation" in f]

        for f in files:
            yield f
    except Exception as e:
        logger.fatal(f"Error while getting data files: {e}")
        raise


def micro_batches_generator(file_path: str, chunk_size: int):
    """Generate micro batches from a CSV file."""
    try:
        df = pd.read_csv(file_path, chunksize=chunk_size)
        for chunk in df:
            yield chunk
    except Exception as e:
        logger.fatal(f"Error while generating micro batch for {file_path}: {e}")
        raise
