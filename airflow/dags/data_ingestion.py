import os
import shutil
import pandas as pd
import datetime
import pendulum

from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Constants
postgres_conn_id = "postgres_connection"
RAW_FOLDER = "/opt/airflow/data/raw"
PROCESSED_FOLDER = "/opt/airflow/data/processed"


@dag(
    dag_id="f1_dynamic_csv_ingest",
    schedule=None,
    start_date=pendulum.datetime(2025, 10, 31, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=20),
    tags=["f1", "bronze"]
)
def F1DynamicCSVIngest():

    @task
    def list_csv_files():
        """Return all CSV files in the raw folder"""
        files = []
        for file in os.listdir(RAW_FOLDER):
            if file.endswith(".csv"):
                files.append(file)
        if not files:
            print("No CSV files found in raw folder")
        return files

    def _process_csv(csv_file: str):
        """Create table dynamically and load CSV data"""
        file_path = os.path.join(RAW_FOLDER, csv_file)
        table_name = os.path.splitext(csv_file)[0].lower()  # removing the .csv extension
        full_table_name = f"bronze.{table_name}"

        #Read CSV headers dynamically
        df = pd.read_csv(file_path, nrows=0)
        columns_sql = ", ".join([f'"{col}" TEXT' for col in df.columns])

        #Generate CREATE TABLE SQL
        create_table_sql = f"""
        CREATE SCHEMA IF NOT EXISTS bronze;
        CREATE TABLE IF NOT EXISTS {full_table_name} (
            {columns_sql}
        );
        """

        #Execute CREATE TABLE and COPY
        hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        conn = hook.get_conn()
        cur = conn.cursor()
        cur.execute(create_table_sql)
        conn.commit()

        with open(file_path, "r") as f:
            cur.copy_expert(
                f"COPY {full_table_name} FROM STDIN WITH CSV HEADER DELIMITER ',' QUOTE '\"'",
                f
            )
        conn.commit()
        cur.close()
        conn.close()

        #Move CSV to processed folder
        os.makedirs(PROCESSED_FOLDER, exist_ok=True)
        shutil.move(file_path, os.path.join(PROCESSED_FOLDER, csv_file))
        print(f"Processed {csv_file} → {full_table_name}")

    #Wrap the plain Python function as an Airflow task
    process_csv = task(_process_csv)

    #Chain tasks
    csv_files = list_csv_files()
    process_csv.expand(csv_file=csv_files)

dag = F1DynamicCSVIngest()
