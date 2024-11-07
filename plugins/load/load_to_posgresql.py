import logging

from airflow.decorators import task
from sqlalchemy.exc import SQLAlchemyError

from utils.data_utils import get_engine

logger = logging.getLogger("airflow.task")

@task
def load_to_postgres(df, table_name):
    engine = get_engine("postgres", "postgres", "localhost", 5432, "etl")
    if not engine:
        logger.error("Failed to create database engine. Task aborted.")
        return

    try:
        df.to_sql(table_name, con=engine, if_exists='replace', index=False)
        logger.info(f"Data successfully written to the '{table_name}' table.")
    except SQLAlchemyError as e:
        logger.error(f"An error occurred while writing to the '{table_name}' table: {e}")
    finally:
        engine.dispose()

