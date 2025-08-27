import os
from dotenv import load_dotenv
load_dotenv()

bucket_name = os.getenv("BUCKET_NAME")

def load_data_from_s3(spark, file_name, sep=","):
    df = spark.read.option("header", True).option("sep", sep).csv(f"s3a://{bucket_name}/{file_name}")
    return df

def load_data_from_postgres(spark, jdbc_url, table, properties):
    df = spark.read.jdbc(url=jdbc_url, table=table, properties=properties)
    return df