from pyspark.sql.types import (StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType)

schemas = {
    "transactions": StructType([
        StructField("TransactionID", StringType(), True),
        StructField("CustomerID", StringType(), True),
        StructField("Montant", DoubleType(), True),
        StructField("DateTransaction", TimestampType(), True),
        StructField("MoyenPaiement", StringType(), True),
        StructField("BICsender", StringType(), True),
        StructField("BICreceiver", StringType(), True)
    ]),

    "comptes_bancaires": StructType([
        StructField("customerid", StringType(), True),
        StructField("account_number", StringType(), True),
        StructField("account_type", StringType(), True),
        StructField("creation_date", DateType(), True)
    ]),

    "info_global": StructType([
        StructField("Number", IntegerType(), True),
        StructField("NameSet", StringType(), True),
        StructField("GivenName", StringType(), True),
        StructField("Surname", StringType(), True),
        StructField("Title", StringType(), True),
        StructField("Gender", StringType(), True),
        StructField("Birthday", DateType(), True),
        StructField("EmailAddress", StringType(), True),
        StructField("TelephoneNumber", StringType(), True),
        StructField("StreetAddress", StringType(), True),
        StructField("City", StringType(), True),
        StructField("ZipCode", StringType(), True),
        StructField("StateFull", StringType(), True),
        StructField("State", StringType(), True),
        StructField("Country", StringType(), True),
        StructField("CountryFull", StringType(), True),
        StructField("TelephoneCountryCode", StringType(), True),
        StructField("CCType", StringType(), True),
        StructField("CCNumber", StringType(), True),
        StructField("CCExpires", StringType(), True),
        StructField("Company", StringType(), True),
        StructField("Occupation", StringType(), True),
        StructField("NationalID", StringType(), True)
    ])
}
