import os
from pyspark.sql.functions import concat_ws, sha2, col, lit, current_date
from pyspark.sql.functions import col, countDistinct, sum as _sum, avg
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from utils.schemas import schemas


# Typage des colonnes
def cast_columns(df: DataFrame, dataset_name: str) -> DataFrame:
    if dataset_name not in schemas:
        raise ValueError(f"Schema not defined for dataset: {dataset_name}")

    schema = schemas[dataset_name]

    for field in schema.fields:
        df = df.withColumn(field.name, df[field.name].cast(field.dataType))
    return df

# Supprimer les lignes contenant des valeurs nulles
def drop_nulls(df, subset=None):
    return df.dropna(subset=subset)


# Supprimer les doublons
def drop_duplicates(df, subset=None):
    return df.dropDuplicates(subset)

# Ajouter une colonne de cle primaire
def add_primary_key(df, cols, new_col="pk_hash"):
    return df.withColumn(new_col, sha2(concat_ws("||", *[col(c) for c in cols]), 256))

# Ajouter les colonnes de validite
def add_validity_columns(df, flag_col="valid_flag", from_col="valid_from", until_col="valid_until"):
    return (df.withColumn(flag_col, lit(True)).withColumn(from_col, current_date()).withColumn(until_col, lit(None).cast("date"))
)

# Jointure des DataFrames
def join_and_process_dataframes(transactions, accounts, info_global):
    accounts_renamed = accounts.withColumnRenamed("customerid", "accounts_customerid")
    info_global_renamed = info_global.withColumnRenamed("Number", "global_Number")
    
    joined_df = transactions.join(accounts_renamed, transactions.CustomerID == accounts_renamed.accounts_customerid, "inner").join(info_global_renamed, transactions.CustomerID == info_global_renamed.global_Number.cast("string"),"inner")
    
    columns_to_keep = [
        "TransactionID", "CustomerID", "Montant", "DateTransaction", "MoyenPaiement",
        "BICsender", "BICreceiver",
        "account_number", "account_type", "creation_date",
        "NameSet", "GivenName", "Surname", "Title", "Gender",
        "Birthday", "EmailAddress", "TelephoneNumber", "StreetAddress", "City",
        "ZipCode", "StateFull", "State", "Country", "CountryFull", "TelephoneCountryCode",
        "CCType", "CCNumber", "CCExpires", "Company", "Occupation", "NationalID"
    ]

    return joined_df.select(columns_to_keep)


# Agrégation par pays, genre et type de compte
def aggregate_by_country_gender_account(df, country_col="Country", gender_col="Gender", account_type_col="account_type", customer_id_col="CustomerID", montant_col="Montant"):
    agg_df = (df.groupBy(country_col, gender_col, account_type_col).agg(
                  countDistinct(col(customer_id_col)).alias("nb_clients"),
                  _sum(col(montant_col)).alias("total_montant")
              )
    )
    return agg_df

# Agrégation par pays, département et moyen de paiement
def aggregate_by_country_department_payment(df, country_col="Country", department_col="State", payment_col="MoyenPaiement", customer_id_col="CustomerID", montant_col="Montant"):
    return df.groupBy(country_col, department_col, payment_col).agg(
        countDistinct(col(customer_id_col)).alias("nb_clients"),_sum(col(montant_col)).alias("total_montant")
    )

def stats_transactions(df, country_col="Country", montant_col="Montant"):
    return df.groupBy(country_col).agg(
        F.avg(montant_col).alias("moyenne"),
        F.expr(f"percentile_approx({montant_col}, 0.5)").alias("mediane"),
        F.expr(f"percentile_approx({montant_col}, array(0.25,0.5,0.75))").alias("quantiles")
    )


def stats_totaux_par_pays(df, montant_col="Montant", country_col="Country"):
    # Somme totale par pays
    df_somme_pays = df.groupBy(country_col).agg(F.sum(montant_col).alias("total_montants"))
    
    # Stats globales sur les totaux
    df_global_stats = df_somme_pays.agg(
        F.avg("total_montants").alias("moyenne"),
        F.expr("percentile_approx(total_montants, 0.5)").alias("mediane"),
        F.expr("percentile_approx(total_montants, array(0.25,0.5,0.75))").alias("quantiles")
    )
    return df_global_stats
