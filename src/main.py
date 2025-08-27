from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv
from etl.extract import load_data_from_s3
from etl.transform import cast_columns
import sys
sys.stdout.reconfigure(encoding='utf-8')

load_dotenv()

# Environment variables
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
BUCKET_NAME = os.getenv("BUCKET_NAME")

# Initialisation de la session Spark
spark = SparkSession.builder \
    .appName("ReadFromS3") \
    .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID) \
    .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .getOrCreate()

# Chargement des données depuis S3
df_transactions = load_data_from_s3(spark, "transactions.csv", sep=";")
df_accounts = load_data_from_s3(spark,"comptes_bancaires.csv",sep=",")
df_info_global = load_data_from_s3(spark,"info_global.csv",sep=",")
#df_fr = load_data_from_s3(spark,"set_fr.csv",sep=";")
#df_global = load_data_from_s3(spark,"set_global.csv",sep=";")

# Ajouter le typage des columns
df_transactions = cast_columns(df_transactions, "transactions")
df_accounts = cast_columns(df_accounts, "comptes_bancaires")
df_info_global = cast_columns(df_info_global, "info_global")


# Affichage des schémas des DataFrames
df_transactions.printSchema()
df_accounts.printSchema()
df_info_global.printSchema()

# Affichage des DataFrames
df_transactions.show(5)
df_accounts.show(5)
df_info_global.show(5)

# Arrêt de la session Spark
spark.stop()