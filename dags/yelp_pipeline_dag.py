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

    bronze >> silver >> gold