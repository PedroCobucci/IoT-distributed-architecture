import io
import fastavro.schema
import fastavro.io
from fastavro import reader
from confluent_kafka import Consumer

c = Consumer({
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'mygroup',
    'auto.offset.reset': 'earliest'
})

c.subscribe(['measurements'])

while True:
    msg = c.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue

    bytes_reader = io.BytesIO(msg.value())
    avro_reader = reader(bytes_reader)
    for record in avro_reader:
        print('Received message: {}'.format(record))

c.close()