from pyspark.sql import DataFrame
from utils.schemas import schemas

def cast_columns(df: DataFrame, dataset_name: str) -> DataFrame:
    if dataset_name not in schemas:
        raise ValueError(f"Schema not defined for dataset: {dataset_name}")

    schema = schemas[dataset_name]

    for field in schema.fields:
        df = df.withColumn(field.name, df[field.name].cast(field.dataType))
    return df
