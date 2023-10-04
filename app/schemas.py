from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

user_schema = StructType(
    [
        StructField("User ID", IntegerType(), True),
        StructField("Name", StringType(), True),
        StructField("Email", StringType(), True),
        StructField("Address", StringType(), True),
        StructField("Phone Number", StringType(), True),
    ]
)

transaction_schema = StructType(
    [
        StructField("Transaction ID", IntegerType(), True),
        StructField("User ID", IntegerType(), True),
        StructField("Item", StringType(), True),
        StructField("Price", DoubleType(), True),
    ]
)
