import pytest
from pyspark.sql import DataFrame, SparkSession

import app.schemas as schemas
import app.utils as utils

spark = SparkSession.builder.appName("Unit Tests").getOrCreate()

test_users_data = [
    (1, "John Smith", "john.smith@example.com", "123 Main St, Anytown USA", "555-1234"),
    (2, "Jane Doe", "jane.doe@example.com", "456 Oak Ave, Anytown USA", "555-5678"),
    (
        3,
        "Bob Johnson",
        "bob.johnson@example.com",
        "789 Elm St, Anytown USA",
        "555-9012",
    ),
]
transactions_data = [
    (1, 1, "Apple", 0.5),
    (2, 1, "Banana", 0.25),
    (3, 2, "Orange", 0.75),
    (4, 3, "Grapes", 1.5),
    (5, 3, "Pineapple", 2.0),
    (6, 4, "Kiwi", 0.75),
]
test_users = spark.createDataFrame(test_users_data, schema=schemas.user_schema)
test_transactions = spark.createDataFrame(
    transactions_data, schema=schemas.transaction_schema
)


@pytest.fixture(scope="session", autouse=True)
def transformed_data() -> DataFrame:
    return utils.transform(test_users, test_transactions)


def test_transformed_output_should_have_correct_columns(
    transformed_data: DataFrame,
) -> None:
    expected_columns = [
        "transaction_id",
        "user_id",
        "item",
        "price",
        "name",
        "email",
        "address",
        "phone_number",
    ]
    assert transformed_data.columns == expected_columns


def test_transformed_output_should_exclude_transactions_with_missing_user_lookup(
    transformed_data: DataFrame,
) -> None:
    num_rows_with_lookup = 5
    assert transformed_data.count() == num_rows_with_lookup
