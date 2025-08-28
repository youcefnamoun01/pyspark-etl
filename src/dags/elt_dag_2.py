from airflow import DAG
from airflow.decorators import task
from datetime import datetime
from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv
from etl.extract import load_data_from_s3
from etl.transform import (
    cast_columns, drop_nulls, drop_duplicates,
    add_primary_key, add_validity_columns,
    join_and_process_dataframes,
    aggregate_by_country_gender_account,
    aggregate_by_country_department_payment,
    stats_transactions
)
from etl.load import load_to_db

load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

FILES = [
    {"name": "transactions", "path": "transactions.csv", "sep": ";"},
    {"name": "accounts", "path": "comptes_bancaires.csv", "sep": ","},
    {"name": "info_global", "path": "info_global.csv", "sep": ","}
]

def init_spark():
    return SparkSession.builder \
        .appName("AirflowETL") \
        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID) \
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
        .getOrCreate()

def get_primary_keys(file_name):
    mapping = {
        "transactions": ["TransactionID", "CustomerID"],
        "accounts": ["customerid", "account_number"],
        "info_global": ["NationalID", "Number"]
    }
    return mapping[file_name]

# --- DAG ---
default_args = {
    'owner': 'youcef',
    'start_date': datetime(2025, 8, 28),
    'retries': 1,
}

dag = DAG(
    'etl_spark_inmemory_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
)

# --- Extraction ---
@task
def extract_file(file):
    spark = init_spark()
    df = load_data_from_s3(spark, file['path'], sep=file['sep'])
    spark.stop()
    return df

extract_tasks = []
for file in FILES:
    extract_tasks.append(extract_file(file))

# --- Transformation ---
@task
def transform_file(file_name, df):
    df = cast_columns(df, file_name)
    df = drop_nulls(df)
    df = drop_duplicates(df)
    df = add_primary_key(df, get_primary_keys(file_name), new_col=f"{file_name}_pk")
    df = add_validity_columns(df)
    return df

transform_tasks = []
for i, file in enumerate(FILES):
    transform_tasks.append(transform_file(file['name'], extract_tasks[i]))

# --- Join & Aggregations ---
@task
def join_and_aggregate(df_transactions, df_accounts, df_info_global):
    df_joined = join_and_process_dataframes(df_transactions, df_accounts, df_info_global)

    df_agg_country_gender_account = aggregate_by_country_gender_account(df_joined)
    df_agg_country_department_payment = aggregate_by_country_department_payment(df_joined)
    df_stats_transact = stats_transactions(df_joined)

    return df_agg_country_gender_account, df_agg_country_department_payment, df_stats_transact

agg_results = join_and_aggregate(transform_tasks[0], transform_tasks[1], transform_tasks[2])

# --- Load final ---
@task
def load_to_db_file(df, table_name):
    load_to_db(df, table_name)

load_to_db_file(agg_results[0], "df_agg_country_gender_account")
load_to_db_file(agg_results[1], "df_agg_country_department_payment")
load_to_db_file(agg_results[2], "df_stats_transact")
