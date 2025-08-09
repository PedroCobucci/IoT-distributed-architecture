import os
import time
import random
from datetime import datetime
import fastavro.schema
import fastavro.io
import io
from confluent_kafka import Producer

# Kafka config
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "measurements")

# Load Avro schema
SCHEMA_PATH = "inverter.avsc"
schema = fastavro.schema.load_schema("inverter.avsc")

# Kafka producer
producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

def generate_measurement():
    voltage = round(random.uniform(210, 230), 2)
    current = round(random.uniform(4, 6), 2)
    power = round(voltage * current, 2)
    return {
        "device_id": f"inverter-{random.randint(1,3)}",
        "voltage": voltage,
        "current": current,
        "power": power
    }

def serialize_avro(data, schema):
    bytes_writer = io.BytesIO()
    fastavro.writer(bytes_writer, schema, [data])
    serialized_data = bytes_writer.getvalue()
    return serialized_data

if __name__ == "__main__":
    print(f"Starting data generator for topic '{KAFKA_TOPIC}'...")
    while True:
        measurement = generate_measurement()
        payload = serialize_avro(measurement, schema)
        print(f"Producing: {measurement}")
        producer.produce(topic=KAFKA_TOPIC, value=payload)
        producer.flush()
        time.sleep(2)
