from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F

path_profiles = "../simulate/data/raw/adv_profiles.parquet"


def load_battle_data(path: str) -> DataFrame:
    spark = SparkSession.builder.master("local").appName("TransformBattles").getOrCreate()
    df = spark.read.parquet(path)
    return df


def compute_strongest_adventurers(df: DataFrame) -> DataFrame:
    data = df.groupBy("adventurer").agg(
        F.count("*").alias("total_battles"),
        F.sum(F.when(F.col("result") == "win", 1).otherwise(0)).alias("wins")
    ).withColumn("win_rate", F.round(F.col("wins")/F.col("total_battles"), 2)).orderBy("win_rate", ascending=False)
    return data


def compute_strongest_monsters(df: DataFrame) -> DataFrame:
    data = df.groupby("monster").agg(
        F.count("*").alias("encounters"),
        F.sum(F.when(F.col("result") == "lose", 1).otherwise(0)).alias("monster_wins")
    ).withColumn("monster_win_rate", F.round(F.col("monster_wins") / F.col("encounters"), 2)).orderBy("monster_win_rate", ascending=False)
    return data


def compute_battle_days(df: DataFrame) -> DataFrame:
    data = df.groupby("timestamp").agg(
        F.count("*").alias("battles"),
        F.sum(F.when(F.col("result") == "win", 1).otherwise(0)).alias("wins")
    ).withColumnRenamed("timestamp", "day").orderBy("day")
    return data


if __name__ == "__main__":
    path_battle = "../simulate/data/raw/battle_logs.parquet"
    df = load_battle_data(path_battle)

    # compute_strongest_adventurers(df).show()
    # compute_strongest_monsters(df).show()
    # compute_battle_days(df).show()

    df.sparkSession.stop()

