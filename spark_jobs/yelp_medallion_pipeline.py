import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count

def main():
    spark = SparkSession.builder \
        .appName("Yelp Medallion Pipeline") \
        .config("spark.driver.memory", "3g") \
        .config("spark.executor.memory", "3g") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

    spark.conf.set("spark.sql.files.maxPartitionBytes", "64MB")

    stage = sys.argv[1] if len(sys.argv) > 1 else "all"

    raw_path = "file:///app/data/landing/reviews_from_kafka.jsonl"

    bronze_path = "file:///app/data/bronze"
    silver_path = "file:///app/data/silver"
    gold_path = "file:///app/data/gold"

    # BRONZE
    if stage in ["bronze", "all"]:
        print("Running BRONZE layer...")

        bronze_df = spark.read.json(raw_path)

        if "review_id" in bronze_df.columns:
            bronze_df = bronze_df.dropDuplicates(["review_id"])

        bronze_df.write.mode("overwrite").parquet(bronze_path)

        print("Bronze layer saved")

    # SILVER
    if stage in ["silver", "all"]:
        print("Running SILVER layer...")

        bronze_df = spark.read.parquet(bronze_path)

        silver_df = bronze_df.select(
            col("business_id"),
            col("stars").cast("double"),
            col("useful").cast("int"),
            col("funny").cast("int"),
            col("cool").cast("int")
        ).filter(
            col("stars").isNotNull()
        )

        silver_df.write.mode("overwrite").parquet(silver_path)

        print("Silver layer saved")

    # GOLD
    if stage in ["gold", "all"]:
        print("Running GOLD layer...")

        silver_df = spark.read.parquet(silver_path)

        gold_df = silver_df.groupBy("business_id").agg(
            avg("stars").alias("avg_stars"),
            count("*").alias("review_count")
        )

        gold_df.write.mode("overwrite").parquet(gold_path)

        print("Gold layer saved")

    spark.stop()


if __name__ == "__main__":
    main()