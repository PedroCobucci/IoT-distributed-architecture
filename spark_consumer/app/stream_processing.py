from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro, to_avro
from pyspark.sql.functions import col
import os
import json

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = KAFKA_TOPIC =  os.getenv("KAFKA_TOPIC", "equipments")
SPARK_MASTER = os.getenv("SPARK_MASTER", "spark://spark:7077")

avro_schema = json.dumps({
    "namespace": "inverter",
    "type": "record",
    "name": "Inverter",
    "fields": [
        {"name": "device_id", "type": "string"},
        {"name": "voltage", "type": "float"},
        {"name": "current", "type": "float"},
        {"name": "power", "type": "float"}
    ]
})

spark = SparkSession.builder \
    .appName("KafkaAvroConsumer") \
    .master(SPARK_MASTER) \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.kryo.registrationRequired", "false") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-avro_2.12:3.5.0") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoints") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

parsed_df = df.select(from_avro(col("value"), avro_schema).alias("data"))

final_df = parsed_df.select("data.*")

query = final_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()