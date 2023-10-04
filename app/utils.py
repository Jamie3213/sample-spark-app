from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def transform(users: DataFrame, transactions: DataFrame) -> DataFrame:
    transactions_and_users = transactions.join(users, "User ID", how="inner")
    return transactions_and_users.select(
        col("Transaction ID").alias("transaction_id"),
        col("User ID").alias("user_id"),
        col("Item").alias("item"),
        col("Price").alias("price"),
        col("Name").alias("name"),
        col("Email").alias("email"),
        col("Address").alias("address"),
        col("Phone Number").alias("phone_number"),
    )
