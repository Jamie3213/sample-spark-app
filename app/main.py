import os

import schemas
import utils
from pyspark.sql import SparkSession


def main() -> None:
    spark = SparkSession.builder.appName("Sample Spark App").getOrCreate()

    users_file_path = os.path.join("data", "input", "users.csv")
    users = spark.read.csv(users_file_path, header=True, schema=schemas.user_schema)
    transactions_file_path = os.path.join("data", "input", "transactions.csv")
    transactions = spark.read.csv(
        transactions_file_path, header=True, schema=schemas.transaction_schema
    )

    users_and_transactions = utils.transform(users, transactions)
    output_file_path = os.path.join("data", "output")
    users_and_transactions.write.csv(output_file_path, header=True, mode="overwrite")


if __name__ == "__main__":
    main()
