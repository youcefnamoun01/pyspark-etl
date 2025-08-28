from pyspark.sql import SparkSession
import os
import sys
from dotenv import load_dotenv
from etl.extract import load_data_from_s3
from etl.transform import cast_columns
from etl.transform import drop_nulls, drop_duplicates, add_primary_key, add_validity_columns, join_and_process_dataframes, aggregate_by_country_gender_account, aggregate_by_country_department_payment, transaction_stats_by_country
from etl.load import load_to_db

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

# Ajouter le typage des columns
df_transactions = cast_columns(df_transactions, "transactions")
df_accounts = cast_columns(df_accounts, "comptes_bancaires")
df_info_global = cast_columns(df_info_global, "info_global")


# Affichage des schemas des DataFrames
df_transactions.printSchema()
df_accounts.printSchema()
df_info_global.printSchema()

# Affichage des DataFrames
df_transactions.show(5)
df_accounts.show(5)
df_info_global.show(5)

# Suppression des valeurs nulles
cleaned_df_transactions = drop_nulls(df_transactions)
cleaned_df_accounts = drop_nulls(df_accounts)
cleaned_df_info_global = drop_nulls(df_info_global, subset=["NationalID", "Number"])


# Suppression des doublons
cleaned_df_transactions = drop_duplicates(cleaned_df_transactions)
cleaned_df_accounts = drop_duplicates(cleaned_df_accounts)
cleaned_df_info_global = drop_duplicates(cleaned_df_info_global)

# Ajout de la cle primaire
cleaned_df_transactions = add_primary_key(cleaned_df_transactions, ["TransactionID", "CustomerID"], new_col="transaction_pk")
cleaned_df_accounts = add_primary_key(cleaned_df_accounts, ["customerid", "account_number"], new_col="account_pk")
cleaned_df_info_global = add_primary_key(cleaned_df_info_global, ["NationalID", "Number"], new_col="global_pk")

# Ajouter les colonnes de validite
df_transactions_sc = add_validity_columns(cleaned_df_transactions)
df_accounts_sc = add_validity_columns(cleaned_df_accounts)
df_info_global_sc = add_validity_columns(cleaned_df_info_global)

# Jointure des DataFrames
df_joined = join_and_process_dataframes(df_transactions_sc, df_accounts_sc, df_info_global_sc)
df_joined.printSchema()

# Aggregations par pays, genre et type de compte
df_agg_country_gender_account = aggregate_by_country_gender_account(df_joined)
df_agg_country_gender_account.show(10)

# Agr?gation par pays, d?partement et moyen de paiement
df_agg_country_department_payment = aggregate_by_country_department_payment(df_joined)
df_agg_country_department_payment.show(10)

df_stats = transaction_stats_by_country(spark, df_joined)
df_stats.show(truncate=False)

load_to_db (df_agg_country_gender_account, "df_agg_country_gender_account")
load_to_db (df_agg_country_department_payment, "df_agg_country_department_payment")
load_to_db (df_stats, "df_stats")

# Arret de la session Spark
spark.stop()