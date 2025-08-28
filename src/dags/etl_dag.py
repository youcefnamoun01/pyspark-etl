from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import os
from etl import extract, transform, load
from pyspark.sql import SparkSession
from dotenv import load_dotenv

load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
BUCKET_NAME = os.getenv("BUCKET_NAME")

def init_spark():
    spark = SparkSession.builder \
        .appName("ReadFromS3") \
        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID) \
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
        .getOrCreate()
    return spark

def extract_data(**kwargs):
    spark = init_spark()
    kwargs['ti'].xcom_push(key='df_transactions', value=extract.load_data_from_s3(spark, "transactions.csv", sep=";"))
    kwargs['ti'].xcom_push(key='df_accounts', value=extract.load_data_from_s3(spark, "comptes_bancaires.csv", sep=","))
    kwargs['ti'].xcom_push(key='df_info_global', value=extract.load_data_from_s3(spark, "info_global.csv", sep=","))
    spark.stop()

def transform_data(**kwargs):
    from pyspark.sql import DataFrame
    ti = kwargs['ti']

    # Récupération des DataFrames depuis XCom
    df_transactions = ti.xcom_pull(key='df_transactions', task_ids='extract_task')
    df_accounts = ti.xcom_pull(key='df_accounts', task_ids='extract_task')
    df_info_global = ti.xcom_pull(key='df_info_global', task_ids='extract_task')

    # Transformations
    df_transactions = transform.cast_columns(df_transactions, "transactions")
    df_accounts = transform.cast_columns(df_accounts, "comptes_bancaires")
    df_info_global = transform.cast_columns(df_info_global, "info_global")

    df_transactions = transform.drop_nulls(df_transactions)
    df_accounts = transform.drop_nulls(df_accounts)
    df_info_global = transform.drop_nulls(df_info_global, subset=["NationalID", "Number"])

    df_transactions = transform.drop_duplicates(df_transactions)
    df_accounts = transform.drop_duplicates(df_accounts)
    df_info_global = transform.drop_duplicates(df_info_global)

    df_transactions = transform.add_primary_key(df_transactions, ["TransactionID", "CustomerID"], new_col="transaction_pk")
    df_accounts = transform.add_primary_key(df_accounts, ["customerid", "account_number"], new_col="account_pk")
    df_info_global = transform.add_primary_key(df_info_global, ["NationalID", "Number"], new_col="global_pk")

    df_transactions = transform.add_validity_columns(df_transactions)
    df_accounts = transform.add_validity_columns(df_accounts)
    df_info_global = transform.add_validity_columns(df_info_global)

    df_joined = transform.join_and_process_dataframes(df_transactions, df_accounts, df_info_global)
    df_agg_country_gender_account = transform.aggregate_by_country_gender_account(df_joined)
    df_agg_country_department_payment = transform.aggregate_by_country_department_payment(df_joined)
    df_stats_transact = transform.stats_transactions(df_joined)

    # Stockage dans XCom pour le load
    ti.xcom_push(key='df_agg_country_gender_account', value=df_agg_country_gender_account)
    ti.xcom_push(key='df_agg_country_department_payment', value=df_agg_country_department_payment)
    ti.xcom_push(key='df_stats_transact', value=df_stats_transact)

def load_data(**kwargs):
    ti = kwargs['ti']
    load_to_db = load.load_to_db
    load_to_db(ti.xcom_pull(key='df_agg_country_gender_account', task_ids='transform_task'), "df_agg_country_gender_account")
    load_to_db(ti.xcom_pull(key='df_agg_country_department_payment', task_ids='transform_task'), "df_agg_country_department_payment")
    load_to_db(ti.xcom_pull(key='df_stats_transact', task_ids='transform_task'), "df_stats_transact")


default_args = {
    'owner': 'youcef',
    'start_date': datetime(2025, 8, 28),
    'retries': 1,
}

dag = DAG(
    'etl_spark_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
)

extract_task = PythonOperator(
    task_id='extract_task',
    python_callable=extract_data,
    provide_context=True,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_task',
    python_callable=transform_data,
    provide_context=True,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_task',
    python_callable=load_data,
    provide_context=True,
    dag=dag
)

extract_task >> transform_task >> load_task
