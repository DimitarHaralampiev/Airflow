import boto3
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from io import StringIO


# Step 1: Read CSV file
def read_csv_file(file_path):
    df = pd.read_csv(file_path)
    return df


# Step 2: Define transformation functions
def drop_duplicates(df):
    return df.drop_duplicates()


def drop_na(df):
    return df.dropna()


def rename_columns(df):
    df.columns = df.columns.str.lower().str.replace(".", "_")
    return df


def drop_columns(df):
    columns_to_drop = ["altitude", "owner_1", "certification_contact", "certification_address", "certification_body"]
    return df.drop(columns=columns_to_drop)


def change_types(df):
    categorical_types = ["species", "owner", "country_of_origin", "farm_name", "lot_number", "mill", "region",
                         "producer", "in_country_partner", "variety", "processing_method", "color",
                         "unit_of_measurement"]
    df[categorical_types] = df[categorical_types].astype("category")

    date_cols = ["harvest_year", "grading_date", "expiration"]
    for col in date_cols:
        df[col] = pd.to_datetime(df[col], errors="coerce")

    return df


def clean_bag_weight_col(df, col):
    df[col] = pd.to_numeric(df[col].str.split(" ").str.get(0), errors="coerce")
    return df


def reset_index(df):
    return df.reset_index(drop=True)


# Step 3: Write to PostgreSQL
def write_to_postgres(df, table_name):
    conn_str = "postgresql+psycopg2://postgres:postgres@localhost:5432/etl"
    engine = create_engine(conn_str)

    try:
        # Create the table if it doesn't exist
        df.to_sql(table_name, con=engine, if_exists='replace', index=False)
        print(f"Data successfully written to the '{table_name}' table.")
    except SQLAlchemyError as e:
        print(f"An error occurred: {e}")

def load_to_s3(df, s3_bucket, s3_key, aws_region="us-west-2"):
    s3_client = boto3.client("s3", region_name=aws_region)
    csv_buffer = StringIO(df.to_csv(index=False))

    try:
        s3_client.put_object(Bucket=s3_bucket, Key=s3_key, Body=csv_buffer.getvalue().encode('utf-8'))
        print(f"File successfully uploaded to s3://{s3_bucket}/{s3_key}")
    except Exception as e:
        print(f"An error occurred: {e}")

def read_from_postgresql():

    conn_str = f"postgresql+psycopg2://postgres:postgres@localhost:5432/etl"
    engine = create_engine(conn_str)
    query = """SELECT * FROM arabica_data_cleaned"""
    try:
        # Use pandas to read the SQL query into a DataFrame
        df = pd.read_sql(query, engine)
        return df
    except Exception as e:
        print(f"An error occurred: {e}")
        return None
    finally:
        engine.dispose()


# Step 4: Define the ETL process
def etl_process(file_path, table_name):
    # Extract
    df = read_csv_file(file_path)

    # Transform
    df = drop_duplicates(df)
    df = drop_na(df)
    df = rename_columns(df)
    df = drop_columns(df)
    df = change_types(df)
    # Assume 'bag_weight' is a column you want to clean, change as needed
    df = clean_bag_weight_col(df, 'bag_weight')  # Replace 'bag_weight' with the correct column name
    df = reset_index(df)
    df.to_csv("transformed_data.csv", index=False)
    # Load
    write_to_postgres(df, table_name)
    load_to_s3(df, "transformed-data", "/home/dimitar/Data Science/cleaningData/transformed_data.csv")


# Main execution
if __name__ == "__main__":
    csv_file_path = "/home/dimitar/Data Science/DataTidiyngAndClening/data/arabica_data_cleaned.csv"  # Change to your CSV file path
    etl_process(csv_file_path, "arabica_data_cleaned")  # Specify the table name in PostgreSQL

    df = read_from_postgresql()
    print(df)