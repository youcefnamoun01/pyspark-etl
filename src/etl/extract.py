from pyspark.sql import SparkSession
import pandas as pd
import os
from dotenv import load_dotenv
load_dotenv()


AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
BUCKET_NAME = os.getenv("BUCKET_NAME")

# Créer la session Spark
spark = SparkSession.builder \
    .appName("ReadFromS3") \
    .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID) \
    .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .config("spark.jars.packages","org.apache.hadoop:hadoop-aws:3.3.6,com.amazonaws:aws-java-sdk-bundle:1.12.500") \
    .getOrCreate()

# Lire le fichier CSV depuis S3
salesDF = spark.read.option("header", "true").csv(f"s3a://{BUCKET_NAME}/transactions.csv")

"""
# Lire les fichiers CSV
df_accounts = spark.read.csv("./data/comptes_bancaires.csv", header=True, inferSchema=False, sep=",")
df_transactions = spark.read.csv("./data/transactions.csv", header=True, inferSchema=False, sep=";")
df_fr = spark.read.csv("./data/set_fr.csv", header=True, inferSchema=False, sep=";")
df_global = spark.read.csv("./data/set_global.csv", header=True, inferSchema=False, sep=";")

# Afficher les schémas
df_accounts.printSchema()
df_transactions.printSchema()
df_fr.printSchema()
df_global.printSchema()
"""

spark.stop()