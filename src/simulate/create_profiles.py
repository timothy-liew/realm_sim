import os
import random
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def create_profiles(output_path="data/raw/adv_profiles.parquet"):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    data = [
        ("Agarth", "Mel Aglir", "2023-01-01"),
        ("Jubal", "Mel Aglir", "2023-02-02"),
        ("Aethan", "Adessa", "2024-01-01"),
        ("Fomorous", "Odarath", "2022-12-16")
    ]
    schema = StructType([
        StructField("adventurer", StringType(), True),
        StructField("hometown", StringType(), True),
        StructField("join_date", StringType(), True)
    ])
    spark = SparkSession.builder.master("local").appName("createProfiles").getOrCreate()
    df = spark.createDataFrame(data, schema)
    df.show()
    df.write.parquet(output_path)
    print(f"Finished writing profiles to {output_path}")

    spark.stop()

if __name__ == "__main__":
    create_profiles()

