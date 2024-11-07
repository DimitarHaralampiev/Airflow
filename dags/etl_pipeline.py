from airflow import DAG
import pendulum
from airflow.utils.task_group import TaskGroup

from plugins.extract.extract_csv import read_csv_file
from plugins.extract.extract_postgres import extract_from_postgresql
from plugins.load.load_to_csv import load_to_csv
from plugins.load.load_to_posgresql import load_to_postgres
from plugins.load.load_to_s3 import load_to_s3
from plugins.transform.transform import transform


with DAG(dag_id='etl_pipeline', start_date=pendulum.now(), schedule="@daily", catchup=False) as dag:

    with TaskGroup("extract_data") as extract_data:
        csv_extract_task = read_csv_file("/home/dimitar/Data Science/DataTidiyngAndClening/data/arabica_data_cleaned.csv")
        postgres_extract_task = extract_from_postgresql()

    with TaskGroup("transform_data") as transform_data:
        transform_csv_data = transform(csv_extract_task)
        transform_postgres_data = transform(postgres_extract_task)

    with TaskGroup("load_data") as load_data:
        load_to_csv(transform_csv_data, "transformed_data.csv")
        load_to_postgres(transform_postgres_data, "arabica_data_cleaned")
        load_to_s3(transform_csv_data, "transformed-data", "/home/dimitar/Data Science/cleaningData/transformed_data.csv")

    extract_data >> transform_data >> load_data