import os
from pyspark.sql import DataFrame

def load_to_db(df: DataFrame, table_name: str, mode: str = "append"):
    """
    Charge un DataFrame PySpark dans une table PostgreSQL via JDBC.
    """

    # Infos PostgreSQL pour ton RDS
    pg_host = "database-1.c92i8ymius9p.eu-north-1.rds.amazonaws.com"
    pg_port = "5432"
    pg_db = os.getenv("PG_DATABASE")     # Nom de ta DB
    pg_user = os.getenv("PG_USER")       # Ton username
    pg_password = os.getenv("PG_PASSWORD") # Ton mot de passe

    if not all([pg_db, pg_user, pg_password]):
        raise ValueError("Définis PG_DATABASE, PG_USER et PG_PASSWORD dans les variables d'environnement.")

    jdbc_url = f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_db}"
    properties = {
        "user": pg_user,
        "password": pg_password,
        "driver": "org.postgresql.Driver"
    }

    # Écriture dans PostgreSQL
    df.write.jdbc(
        url=jdbc_url,
        table=table_name,
        mode=mode,
        properties=properties,
        batchsize=10000
    )
    print(f"[INFO] DataFrame chargé dans la table {table_name}")
