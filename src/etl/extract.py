import os
from dotenv import load_dotenv
load_dotenv()

bucket_name = os.getenv("BUCKET_NAME")

# Chargement des données depuis S3
def load_data_from_s3(spark, file_name, sep=","):
    df = spark.read.option("header", True).option("sep", sep).csv(f"s3a://{bucket_name}/{file_name}")
    return df

# Chargement des données depuis PostgreSQL
def load_data_from_db(spark, table_name):
    pg_host = os.getenv("PG_HOST")
    pg_port = os.getenv("PG_PORT", "5432")
    pg_db = os.getenv("PG_DATABASE")
    pg_user = os.getenv("PG_USER")
    pg_password = os.getenv("PG_PASSWORD")

    jdbc_url = f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_db}"
    properties = {
        "user": pg_user,
        "password": pg_password,
        "driver": "org.postgresql.Driver"
    }

    df = spark.read.jdbc(
        url=jdbc_url,
        table=table_name,
        properties=properties
    )

    print(f"[INFO] Table {table_name} chargée en DataFrame PySpark ({df.count()} lignes)")
    return df
