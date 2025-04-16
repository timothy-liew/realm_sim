from pyspark.sql import SparkSession
import pytest
from chispa import assert_df_equality
from src.etl.transform import compute_strongest_adventurers


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local").appName("TestTransform").getOrCreate()


def test_compute_strongest_adventurers(spark):
    input_data = [
        ("Beru", "Ahjin", 8, "Dragon", 8, "win", "2024-03-03"),
        ("Igris", "Ahjin", 8, "Knight", 7, "win", "2024-06-06"),
        ("Igris", "Ahjin", 8, "Jinwoo", 18, "lose", "2025-01-01")
    ]
    schema = "adventurer string, guild string, level int, monster string, danger_level int, result string, timestamp string"
    df = spark.createDataFrame(input_data, schema)
    result = compute_strongest_adventurers(df)

    expected = spark.createDataFrame([
        ("Beru", "Ahjin", 1, 1, 1.0),
        ("Igris", "Ahjin", 2, 1, 0.5)
    ], schema="adventurer string, guild string, total_battles long, wins long, win_rate double")

    assert_df_equality(result, expected, ignore_row_order=True, ignore_nullable=True)
