import json
import os
import sys
from kafka import KafkaProducer, KafkaConsumer


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("YELP_KAFKA_TOPIC", "yelp_reviews_raw")

RAW_PATH = os.getenv(
    "YELP_RAW_PATH",
    "/app/data/raw/yelp_academic_dataset_review.json"
)

LANDING_PATH = os.getenv(
    "YELP_LANDING_PATH",
    "/app/data/landing/reviews_from_kafka.jsonl"
)


def produce(run_id: str) -> None:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda value: value.encode("utf-8") if value else None,
    )

    records_sent = 0

    with open(RAW_PATH, "r", encoding="utf-8") as file:
        for line in file:
            if not line.strip():
                continue

            review = json.loads(line)
            review_id = review.get("review_id")

            event = {
                "run_id": run_id,
                "event_type": "REVIEW",
                "payload": review,
            }

            producer.send(
                KAFKA_TOPIC,
                key=review_id,
                value=event,
            )

            records_sent += 1

            if records_sent % 10000 == 0:
                print(f"Sent {records_sent} records...", flush=True)

    end_event = {
        "run_id": run_id,
        "event_type": "END_OF_RUN",
        "payload": None,
    }

    producer.send(
        KAFKA_TOPIC,
        key=f"END_{run_id}",
        value=end_event,
    )

    producer.flush()
    producer.close()

    print(f"Sent {records_sent} records to Kafka topic: {KAFKA_TOPIC}")


def consume(run_id: str) -> None:
    os.makedirs(os.path.dirname(LANDING_PATH), exist_ok=True)

    temp_path = f"{LANDING_PATH}.tmp"

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id=f"yelp-ingestion-{run_id}",
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        consumer_timeout_ms=600000,
    )

    records_written = 0

    with open(temp_path, "w", encoding="utf-8") as output_file:
        for message in consumer:
            event = message.value

            if event.get("run_id") != run_id:
                continue

            if event.get("event_type") == "END_OF_RUN":
                print(f"Received END_OF_RUN for run_id={run_id}")
                break

            payload = event.get("payload")

            if payload is None:
                continue

            output_file.write(json.dumps(payload) + "\n")
            records_written += 1

            if records_written % 10000 == 0:
                print(f"Wrote {records_written} records...", flush=True)

    consumer.close()

    if os.path.exists(LANDING_PATH):
        os.remove(LANDING_PATH)

    os.replace(temp_path, LANDING_PATH)

    print(f"Wrote {records_written} records to landing path: {LANDING_PATH}", flush=True)


def main() -> None:
    if len(sys.argv) < 3:
        raise ValueError(
            "Usage: python3 yelp_queue_ingestion.py [produce|consume] <run_id>"
        )

    mode = sys.argv[1]
    run_id = sys.argv[2]

    if mode == "produce":
        produce(run_id)
    elif mode == "consume":
        consume(run_id)
    else:
        raise ValueError(f"Unknown mode: {mode}")


if __name__ == "__main__":
    main()