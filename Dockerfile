FROM apache/spark:latest

USER root

RUN apt-get update && apt-get install -y python3-pip

RUN pip3 install pyspark pandas pyarrow

WORKDIR /app

ENV PYSPARK_PYTHON=python3

CMD ["python3", "spark_jobs/yelp_medallion_pipeline.py"]