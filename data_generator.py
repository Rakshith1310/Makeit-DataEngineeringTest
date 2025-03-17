import json
import time
import random
import uuid
import argparse
from kafka import KafkaProducer

def generate_event(include_device_type=False, generate_bad_data=False):
    event_type = random.choice(["page_view", "button_click"])
    user_id = random.randint(1, 1000)
    if generate_bad_data and random.random() < 0.05:  # 5% chance of bad data
        user_id = -1 * user_id  # Invalid user ID
    page_url = f"/page/{random.randint(1, 100)}"
    button_id = str(uuid.uuid4())
    timestamp = int(time.time())
    country = random.choice(["US", "CA", "UK", "DE", "FR", "JP", "XX"]) # XX is invalid
    if generate_bad_data and random.random() < 0.05:
      country = "ZZ" # very invalid.

    event = {
        "event_type": event_type,
        "user_id": user_id,
        "page_url": page_url,
        "button_id": button_id,
        "timestamp": timestamp,
        "country": country
    }

    if include_device_type:
        event["device_type"] = random.choice(["mobile", "desktop", "tablet"])

    return json.dumps(event).encode('utf-8')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Generate and send events to Kafka.')
    parser.add_argument('--topic', type=str, default='my_events', help='Kafka topic to send events to.')
    parser.add_argument('--rps', type=int, default=10, help='Records per second to generate.')
    parser.add_argument('--duration', type=int, default=60, help='Duration (seconds) to run the generator.')
    parser.add_argument('--kafka_broker', type=str, default='localhost:9092', help='Address of kafka broker')

    args = parser.parse_args()

    producer = KafkaProducer(bootstrap_servers=[args.kafka_broker])

    start_time = time.time()
    events_sent = 0
    include_device_type = False
    try:
        while time.time() - start_time < args.duration:
            # Simulate schema evolution after 30 seconds
            if time.time() - start_time > 30:
                include_device_type = True

            for _ in range(args.rps):
                event = generate_event(include_device_type, generate_bad_data=True)
                producer.send(args.topic, event)
                events_sent += 1
            time.sleep(1)
        print(f"Sent {events_sent} events to topic {args.topic} in {args.duration} seconds.")

    except KeyboardInterrupt:
        print("Stopped by user.")
    finally:
        producer.close()
