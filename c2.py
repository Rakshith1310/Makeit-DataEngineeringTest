import json
from kafka import KafkaConsumer
from clickhouse_driver import Client
from datetime import datetime

# Kafka & ClickHouse Configuration
KAFKA_BROKER = "localhost:9092"
TOPICS = ["enriched_events", "country_event_averages", "dlq"]

CLICKHOUSE_HOST = "localhost"
CLICKHOUSE_DATABASE = "events_db"

TABLES = {
    "enriched_events": "enriched_events_raw",
    "country_event_averages": "country_event_averages_raw",
    "dlq": "dlq_table"
}

# Initialize ClickHouse Client
clickhouse_client = Client(host=CLICKHOUSE_HOST)


consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers=[KAFKA_BROKER],
    group_id="clickhouse_consumer_group",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
)

print("Consuming messages from Kafka and inserting into ClickHouse...")

def parse_datetime(value):
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%f") if value else None
    except ValueError:
        return None

# Process Messages from Kafka
for message in consumer:
    topic = message.topic
    event = message.value
    print(f"Processing event from {topic}: {event}")

    try:
        if topic == "enriched_events":
            user_id = event.get("user_id", -1)
            country = event.get("country", "UNKNOWN")
            device_type = event.get("device_type", "unknown")
            timestamp = parse_datetime(event.get("timestamp"))

            if not timestamp:
                raise ValueError(f"Invalid timestamp format: {event}")

            clickhouse_client.execute(
                f"INSERT INTO {CLICKHOUSE_DATABASE}.{TABLES[topic]} (user_id, country, device_type, timestamp) VALUES",
                [(user_id, country, device_type, timestamp)]
            )

        elif topic == "country_event_averages":
            country = event.get("country", "UNKNOWN")
            window_start = parse_datetime(event.get("window_start"))
            window_end = parse_datetime(event.get("window_end"))
            average_events = event.get("average_events", None)

            if not window_start or not window_end:
                raise ValueError(f"Invalid datetime format: {event}")

            clickhouse_client.execute(
                f"INSERT INTO {CLICKHOUSE_DATABASE}.{TABLES[topic]} (country, window_start, window_end, average_events) VALUES",
                [(country, window_start, window_end, average_events)]
            )

        elif topic == "dlq":
            event_data = json.dumps(event)  # Store full event as raw JSON
            timestamp = datetime.utcnow()   # Default to now

            clickhouse_client.execute(
                f"INSERT INTO {CLICKHOUSE_DATABASE}.{TABLES[topic]} (event_time, raw_event) VALUES",
                [(timestamp, event_data)]
            )

        print(f"Inserted into {TABLES[topic]}: {event}")

    except Exception as e:
        print(f"Error inserting into ClickHouse ({TABLES[topic]}): {e}")

# Close consumer
consumer.close()
