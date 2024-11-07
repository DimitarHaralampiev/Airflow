import pandas as pd
from airflow.decorators import task

@task
def drop_duplication(df):
    return df.drop_duplicates()

@task
def drop_na(df):
    return df.dropna()

@task
def rename_columns(df):
    df.columns = df.columns.str.lower().str.replace(".", "_")
    return df

@task
def drop_columns(df):
    df = df.drop(columns = ["altitude", "owner_1", "certification_contact", "certification_address", "certification_body"])
    return df

@task
def change_types(df):
    categorical_types = [
        "species", "owner", "country_of_origin", "farm_name", "lot_number", "mill",
        "region", "producer", "in_country_partner", "variety", "processing_method", "color", "unit_of_measurement"
    ]
    df[categorical_types] = df[categorical_types].astype("category")

    date_cols = ["harvest_year", "grading_date", "expiration"]
    for col in date_cols:
        df[col] = pd.to_datetime(df[col], errors="coerce")

    return df

@task
def clean_bag_weight_col(df, col):
    df[col] = pd.to_numeric(df[col].str.split(" ").str.get(0), errors="coerce")
    return df

@task
def reset_index(df):
    df = df.reset_index()
    return df

@task
def transform(csv_data):
    drop_na_data = drop_na(csv_data)
    drop_duplicate_data = drop_duplication(drop_na_data)
    rename_columns_data = rename_columns(drop_duplicate_data)
    drop_columns_data = drop_columns(rename_columns_data)
    change_types_data = change_types(drop_columns_data)
    clean_bag_weight_data = clean_bag_weight_col(change_types_data)
    return clean_bag_weight_data
