import logging
import pandas as pd

from airflow.decorators import task

logger = logging.getLogger("airflow.task")


@task
def read_csv_file(file_path):
    try:
        df = pd.read_csv(file_path)
        logger.info(f"Successfully read file: {file_path}")
        return df
    except Exception as e:
        logger.error(f"Error reading file {file_path}: {e}")
        raise e
