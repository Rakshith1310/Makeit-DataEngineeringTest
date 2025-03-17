import json
import time
from kafka import KafkaConsumer, KafkaProducer
from collections import defaultdict, deque
from datetime import datetime, timedelta

# Kafka Configuration
KAFKA_BROKER = "localhost:9092"
INPUT_TOPIC = "user_events"
ENRICHED_TOPIC = "enriched_events"
AGGREGATED_TOPIC = "country_event_averages"
DLQ_TOPIC = "dlq"
WINDOW_SIZE = timedelta(minutes=5)


# Kafka Producers
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Kafka Consumer
consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    group_id="my_python_consumer_group",
    auto_offset_reset="earliest"
)

# Data Structures for Rolling Aggregation
window_data = defaultdict(deque)

# Function to compute rolling average
def compute_rolling_average():
    current_time = datetime.utcnow()
    country_averages = {}
    
    for country, events in window_data.items():
        # Remove outdated events
        while events and events[0] < (current_time - WINDOW_SIZE):
            events.popleft()
        
        if events:
            avg = len(events) / WINDOW_SIZE.total_seconds() * 60  # Events per minute
            country_averages[country] = avg
    
    return country_averages

# Consume messages
for message in consumer:
    print("Received a message:", message.value)
    event = message.value
    event_time = datetime.utcnow()
    
    # Validation Checks
    user_id = event.get("user_id", -1)
    country = event.get("country", "")
    device_type = event.get("device_type", "unknown")  # Schema Evolution Handling
    
    if user_id < 0 or country in {"ZZ", "XX"}:
        producer.send(DLQ_TOPIC, {"error": "Invalid data", "event": event})
        continue
    
    # Enriched Event
    enriched_event = {
        "user_id": user_id,
        "country": country,
        "device_type": device_type,
        "timestamp": event_time.isoformat()
    }
    producer.send(ENRICHED_TOPIC, enriched_event)
    
    # Rolling Aggregation
    window_data[country].append(event_time)
    rolling_averages = compute_rolling_average()
    
    # Send Aggregated Data
    for country, avg in rolling_averages.items():
        aggregated_data = {
            "country": country,
            "window_start": (event_time - WINDOW_SIZE).isoformat(),
            "window_end": event_time.isoformat(),
            "average_events": round(avg, 2)
        }
        producer.send(AGGREGATED_TOPIC, aggregated_data)
    
    print(f"Processed event: {enriched_event}")

