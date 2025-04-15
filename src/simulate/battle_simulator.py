import json
import os
import random
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from src.core.adventurer import Adventurer
from src.core.monster import Monster
from src.core.battle import Battle

adventurer_names = ["Elira", "Borin", "Thalara", "Duncan", "Seraphine", "Kael"]
guilds = ["Iron Fangs", "Moonlight Pact", "Arcane Blades"]
monsters = ["Goblin", "Orc", "Wyvern", "Skeleton", "Shadow Fiend"]

def random_date(start_date_str, end_date_str):
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

    delta = end_date - start_date
    random_days = random.randint(0, delta.days)
    random_dt = start_date + timedelta(days=random_days)

    return random_dt.strftime("%Y-%m-%d")


def simulate_battles(n=500, output_path="data/raw/battle_logs.parquet"):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    records = []

    for _ in range(n):
        name = random.choice(adventurer_names)
        guild = random.choice(guilds)
        level = random.randint(1, 10)
        monster_type = random.choice(monsters)
        danger_level = random.randint(1, 7)

        adv = Adventurer(name, guild, level)
        mon = Monster(monster_type, danger_level)
        battle = Battle(adv, mon)

        outcome = battle.simulate()
        outcome["timestamp"] = random_date("2025-04-01", "2025-04-30")
        records.append(outcome)

    spark = (SparkSession.builder.master("local").appName("SimulateBattles").getOrCreate())
    schema = StructType([
        StructField("adventurer", StringType(), True),
        StructField("guild", StringType(), True),
        StructField("level", IntegerType(), True),
        StructField("monster", StringType(), True),
        StructField("danger_level", IntegerType(), True),
        StructField("result", StringType(), True),
        StructField("timestamp", StringType(), True),
    ])
    df = spark.createDataFrame(records, schema)
    df.show()
    df.write.mode("overwrite").parquet(output_path)

    print(f"Simulated {n} battles and saved to {output_path}")
    spark.stop()


if __name__ == "__main__":
    simulate_battles()
