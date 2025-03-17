# Makeit-DataEngineeringTest

## Prerequisites
- Docker
- Python
- Kafka
- ClickHouse

##  Setting Up the Environment

### 1️. Single-Container Setup: Kafka & ClickHouse
We use a single container to run Kafka and ClickHouse for local testing. 


### 2️. Creating Kafka Topics
We define three Kafka topics:
- `user_events`: Raw user event data.
- `enriched_events`: Processed and cleaned event data.
- `country_event_averages`: Rolling averages of events per country.
- `dlq`: (Dead Letter Queue) Stores invalid records for debugging.

Create the topics:
```
/usr/local/kafka/bin/kafka-topics. --create --topic user_events --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1
```

Verify topic creation:
```
/usr/local/kafka/bin/kafka-topics. --list --bootstrap-server localhost:9092
```

##  Streaming Data & Processing

### 3️. Kafka Consumer & Producer
A Python script consumes events from `user_events`, validates them, and routes them to the appropriate topics.

Run the producer (to simulate incoming events):
```
python data_generator.py
```

### 4️. Kafka Streams Application
A Kafka Streams app processes data in real-time:
- Computes a rolling average of event counts per country over a 5-minute window.
- Handles schema evolution.
- Filters invalid records.

## Storing & Querying Data in ClickHouse

### 5. Setting Up ClickHouse Tables
We define tables to store processed data.

Connect to ClickHouse:
```
clickhouse-client
```

Create the Database
```
create database events_db;
```

Create tables:
```
CREATE TABLE events_db.user_events (
    user_id Int64,
    country String,
    event_time DateTime,
    event_type String,
    device_type String DEFAULT 'unknown'
) ENGINE = MergeTree()
ORDER BY event_time;
```

Similar queries for `enriched_events` and `country_event_averages`.


### 6. Validate data ingestion:
```
SELECT * FROM user_events LIMIT 10;
```
