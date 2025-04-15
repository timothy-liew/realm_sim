from pyspark.sql import SparkSession

def inspect_parquet(parquet_path: str, limit: int = 10):
    spark = SparkSession.builder.master("local").appName("InspectParquet").getOrCreate()
    df = spark.read.parquet(parquet_path)
    df.show(limit, truncate=False)
    spark.stop()

if __name__ == "__main__":
    inspect_parquet("data/raw/battle_logs.parquet")