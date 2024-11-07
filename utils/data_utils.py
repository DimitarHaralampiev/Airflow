import logging

from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger("airflow.task")

def get_engine(username, password, host, port, database):
    conn_str = f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}"
    try:
        engine = create_engine(conn_str)
        with engine.connect() as connection:
            logger.info("Successfully connected to the database.")
        return engine
    except SQLAlchemyError as e:
        logger.error(f"SQLAlchemy error occurred while creating engine: {e}")
        return None
    except Exception as ex:
        logger.error(f"Unexpected error occurred while creating engine: {ex}")
        return None