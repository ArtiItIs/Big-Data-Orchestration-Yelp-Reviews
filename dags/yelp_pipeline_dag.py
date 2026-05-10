from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "you",
    "start_date": datetime(2024, 1, 1),
    "retries": 1
}

with DAG(
    dag_id="yelp_medallion_pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    inject_raw_to_kafka = BashOperator(
        task_id="inject_raw_to_kafka",
        bash_command="docker exec spark-app python3 /app/spark_jobs/yelp_queue_ingestion.py produce '{{ run_id }}'"
    )

    consume_kafka_to_landing = BashOperator(
        task_id="consume_kafka_to_landing",
        bash_command="docker exec spark-app python3 /app/spark_jobs/yelp_queue_ingestion.py consume '{{ run_id }}'"
    )

    bronze = BashOperator(
        task_id="bronze_layer",
        bash_command="docker exec spark-app python3 /app/spark_jobs/yelp_medallion_pipeline.py bronze"
    )

    silver = BashOperator(
        task_id="silver_layer",
        bash_command="docker exec spark-app python3 /app/spark_jobs/yelp_medallion_pipeline.py silver"
    )

    gold = BashOperator(
        task_id="gold_layer",
        bash_command="docker exec spark-app python3 /app/spark_jobs/yelp_medallion_pipeline.py gold"
    )

    inject_raw_to_kafka >> consume_kafka_to_landing >> bronze >> silver >> gold