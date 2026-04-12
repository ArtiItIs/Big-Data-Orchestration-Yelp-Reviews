# Yelp Big Data Orchestration Project Pipeline

End-to-end **data engineering pipeline** built using **Apache Spark**, **Apache Airflow**, and Docker.  
The project demonstrates a modern **medallion architecture (Bronze → Silver → Gold)** applied to large-scale Yelp dataset processing.

### Architecture

The system is orchestrated using Apache Airflow and processes data using Spark in a Dockerized environment.
```
graph
A[Yelp Dataset] --> B[Airflow DAG]
B --> C[Spark Job]
C --> D[Bronze Layer]
D --> E[Silver Layer]
E --> F[Gold Layer]
```

### Tech Stack
- Apache Spark (PySpark)
- Apache Airflow
- Docker
- Python
- Yelp Dataset (JSON ~GB scale)

### How to run

docker compose up --build

Open Airflow UI:
http://localhost:8080

Login:
airflow
airflow

Choose 'yelp_medallion_pipeline' in the DAGs on the Airflow site

Press Trigger DAG

Pipeline Description

### Bronze Layer

- Raw ingestion of Yelp JSON
- Converted to Parquet format

### Silver Layer

- Data cleaning
- Type casting
- Filtering invalid rows

### Gold Layer

- Aggregated business metrics
- Average ratings per business
- Review counts

### Key Concepts Demonstrated
- Orchestration
- Distributed data processing
- Medallion architecture
- Containerization
- Batch processing pipelines