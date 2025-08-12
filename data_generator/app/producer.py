import os
import time
import random
import io
from confluent_kafka import Producer
import fastavro
import fastavro.schema
from datetime import datetime
import json

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC =  os.getenv("KAFKA_TOPIC", "equipments")

SCHEMA_FILE = "./app/schemas/inverter.avsc" 
try:
    with open(SCHEMA_FILE, "r") as f:
        schema_json = f.read()
    schema = fastavro.schema.parse_schema(json.loads(schema_json))
    print(f"Schema loaded successfully from {SCHEMA_FILE}")
except FileNotFoundError:
    print(f"Error: The schema file '{SCHEMA_FILE}' was not found.")
    exit(1)
except Exception as e:
    print(f"Error parsing the schema file: {e}")
    exit(1)

producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

def generate_measurement():
    voltage = round(random.uniform(210, 230), 2)
    current = round(random.uniform(4, 6), 2)
    power = round(voltage * current, 2)
    return {
        "device_id": f"inverter-{random.randint(1,3)}",
        "voltage": voltage,
        "current": current,
        "power": power,
    }

def serialize_avro(data, schema):
    bytes_writer = io.BytesIO()
    fastavro.schemaless_writer(bytes_writer, schema, data)
    return bytes_writer.getvalue()

if __name__ == "__main__":
    print(f"Starting data generator for topic '{KAFKA_TOPIC}'...")
    try:
        while True:
            measurement = generate_measurement()
            
            payload = serialize_avro(measurement, schema)
            
            print(f"Producing: {measurement}")
            producer.produce(topic=KAFKA_TOPIC, value=payload)
            producer.flush()
            
            time.sleep(2)
    except KeyboardInterrupt:
        print("Shutting down producer...")
    finally:
        producer.flush()