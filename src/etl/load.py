import os
from pyspark.sql import DataFrame

# Charger les données dans PostgreSQL
def load_to_db(df: DataFrame, table_name: str, mode: str = "overwrite"):
    pg_host = os.getenv("PG_HOST")
    pg_port = os.getenv("PG_PORT", "5432")
    pg_db = os.getenv("PG_DATABASE")
    pg_user = os.getenv("PG_USER")
    pg_password = os.getenv("PG_PASSWORD")

    jdbc_url = f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_db}"
    properties = {
        "user": pg_user,
        "password": pg_password,
        "driver": "org.postgresql.Driver",
        "batchsize": "10000"
    }

    try:
        df.write.jdbc(
            url=jdbc_url,
            table=table_name,
            mode=mode,
            properties=properties
        )
        print(f"[INFO] DataFrame chargé dans la table {table_name} ({df.count()} lignes)")
    except Exception as e:
        print(f"[ERREUR] Échec du chargement de {table_name} : {e}")
        raise
