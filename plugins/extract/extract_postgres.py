import pandas as pd
from sqlalchemy import create_engine


def extract_from_postgresql():
    conn_str = f"postgresql+psycopg2://postgres:postgres@localhost:5432/MovieAppNew"
    engine = create_engine(conn_str)
    query = """SELECT * FROM movie_movie"""
    try:
        # Use pandas to read the SQL query into a DataFrame
        df = pd.read_sql(query, engine)
        return df
    except Exception as e:
        print(f"An error occurred: {e}")
        return None
    finally:
        engine.dispose()