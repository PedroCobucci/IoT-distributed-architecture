import os
import io
from confluent_kafka import Consumer, KafkaException
import fastavro
import fastavro.schema
import json

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC =  os.getenv("KAFKA_TOPIC", "equipments")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "inverter-group")

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

consumer = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "group.id": KAFKA_GROUP_ID,
    "auto.offset.reset": "earliest",
})

consumer.subscribe([KAFKA_TOPIC])

try:
    print(f"Subscribed to topic '{KAFKA_TOPIC}'. Waiting for messages...")
    while True:
        message = consumer.poll(1.0)
        if message is None:
            continue
        if message.error():
            if message.error().code() == KafkaException._PARTITION_EOF:
                continue
            else:
                print(f"Consumer error: {message.error()}")
                break

        message_bytes = io.BytesIO(message.value())

        decoded_record = fastavro.schemaless_reader(message_bytes, schema)
        
        print(f"Decoded Avro record: {decoded_record}")
        
except KeyboardInterrupt:
    print("Shutting down consumer...")
finally:
    consumer.close()