from airflow.decorators import task


@task
def load_to_csv(df, csv_file):
    df.to_csv(csv_file)