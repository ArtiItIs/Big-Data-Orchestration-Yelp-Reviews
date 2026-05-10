import json
import os
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def calculate_percentage(numerator, denominator):
    if denominator == 0:
        return 0.0
    return round((numerator / denominator) * 100, 2)


def build_metric(name, definition, current_value, threshold, cadence, status):
    return {
        "metric_name": name,
        "definition": definition,
        "current_value": current_value,
        "expected_threshold": threshold,
        "update_cadence": cadence,
        "status": status
    }


def main():
    spark = SparkSession.builder \
        .appName("Yelp Data Quality Metrics") \
        .config("spark.driver.memory", "3g") \
        .config("spark.executor.memory", "3g") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

    bronze_path = "file:///app/data/bronze"
    silver_path = "file:///app/data/silver"
    gold_path = "file:///app/data/gold"

    output_dir = "/app/data/quality"
    output_path = f"{output_dir}/data_quality_metrics.json"

    os.makedirs(output_dir, exist_ok=True)

    print("Reading Bronze layer...", flush=True)
    bronze_df = spark.read.parquet(bronze_path)

    print("Reading Silver layer...", flush=True)
    silver_df = spark.read.parquet(silver_path)

    print("Reading Gold layer...", flush=True)
    gold_df = spark.read.parquet(gold_path)

    metrics = []

    # 1. Bronze row count
    print("Calculating Bronze row count...", flush=True)
    bronze_row_count = bronze_df.count()

    metrics.append(
        build_metric(
            name="Bronze row count",
            definition="Number of records available in the Bronze layer.",
            current_value=bronze_row_count,
            threshold="> 0",
            cadence="Every pipeline run",
            status="PASS" if bronze_row_count > 0 else "FAIL"
        )
    )

    # 2. Bronze review_id uniqueness
    print("Calculating Bronze review_id uniqueness...", flush=True)

    if "review_id" in bronze_df.columns:
        bronze_review_id_non_null = bronze_df.filter(
            col("review_id").isNotNull()
        ).count()

        bronze_unique_review_ids = bronze_df.select("review_id") \
            .where(col("review_id").isNotNull()) \
            .distinct() \
            .count()

        review_id_uniqueness = calculate_percentage(
            bronze_unique_review_ids,
            bronze_review_id_non_null
        )
    else:
        bronze_review_id_non_null = 0
        bronze_unique_review_ids = 0
        review_id_uniqueness = 0.0

    metrics.append(
        build_metric(
            name="Bronze review_id uniqueness",
            definition="Percentage of unique review_id values among non-null review_id records in Bronze.",
            current_value=f"{review_id_uniqueness}%",
            threshold="> 99.5%",
            cadence="Every pipeline run",
            status="PASS" if review_id_uniqueness > 99.5 else "FAIL"
        )
    )

    # 3. Silver business_id completeness
    print("Calculating Silver business_id completeness...", flush=True)

    silver_row_count = silver_df.count()
    silver_business_id_not_null = silver_df.filter(
        col("business_id").isNotNull()
    ).count()

    business_id_completeness = calculate_percentage(
        silver_business_id_not_null,
        silver_row_count
    )

    metrics.append(
        build_metric(
            name="Silver business_id completeness",
            definition="Percentage of rows in Silver where business_id is not null.",
            current_value=f"{business_id_completeness}%",
            threshold="> 99%",
            cadence="Every pipeline run",
            status="PASS" if business_id_completeness > 99 else "FAIL"
        )
    )

    # 4. Silver stars validity
    print("Calculating Silver stars validity...", flush=True)

    silver_valid_stars = silver_df.filter(
        (col("stars") >= 1) & (col("stars") <= 5)
    ).count()

    stars_validity = calculate_percentage(
        silver_valid_stars,
        silver_row_count
    )

    metrics.append(
        build_metric(
            name="Silver stars validity",
            definition="Percentage of rows in Silver where stars value is between 1 and 5.",
            current_value=f"{stars_validity}%",
            threshold="100%",
            cadence="Every pipeline run",
            status="PASS" if stars_validity == 100.0 else "FAIL"
        )
    )

    # 5. Gold avg_stars validity
    print("Calculating Gold avg_stars validity...", flush=True)

    gold_row_count = gold_df.count()
    gold_valid_avg_stars = gold_df.filter(
        (col("avg_stars") >= 1) & (col("avg_stars") <= 5)
    ).count()

    avg_stars_validity = calculate_percentage(
        gold_valid_avg_stars,
        gold_row_count
    )

    metrics.append(
        build_metric(
            name="Gold avg_stars validity",
            definition="Percentage of rows in Gold where avg_stars value is between 1 and 5.",
            current_value=f"{avg_stars_validity}%",
            threshold="100%",
            cadence="Every pipeline run",
            status="PASS" if avg_stars_validity == 100.0 else "FAIL"
        )
    )

    # 6. Gold row count
    print("Calculating Gold row count...", flush=True)

    metrics.append(
        build_metric(
            name="Gold row count",
            definition="Number of business-level records available in the Gold layer.",
            current_value=gold_row_count,
            threshold="> 0",
            cadence="Every pipeline run",
            status="PASS" if gold_row_count > 0 else "FAIL"
        )
    )

    result = {
        "product_name": "Yelp Business Review Metrics",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "layers": {
            "bronze_path": "/app/data/bronze",
            "silver_path": "/app/data/silver",
            "gold_path": "/app/data/gold"
        },
        "metrics": metrics
    }

    temp_path = f"{output_path}.tmp"

    print("Saving data quality metrics...", flush=True)

    with open(temp_path, "w", encoding="utf-8") as file:
        json.dump(result, file, indent=2)

    if os.path.exists(output_path):
        os.remove(output_path)

    os.replace(temp_path, output_path)

    print(f"Data quality metrics saved to {output_path}", flush=True)

    for metric in metrics:
        print(
            f"{metric['metric_name']}: {metric['current_value']} "
            f"({metric['status']})",
            flush=True
        )

    spark.stop()


if __name__ == "__main__":
    main()