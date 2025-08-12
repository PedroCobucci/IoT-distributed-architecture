FROM bitnami/spark:3.5

WORKDIR /app

# Install required Kafka client libraries
RUN curl -o /opt/bitnami/spark/jars/kafka-clients-3.6.0.jar \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.6.0/kafka-clients-3.6.0.jar && \
    curl -o /opt/bitnami/spark/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar && \
    curl -o /opt/bitnami/spark/jars/spark-avro_2.12-3.5.0.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-avro_2.12/3.5.0/spark-avro_2.12-3.5.0.jar && \
    curl -o /opt/bitnami/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "./app/stream_processing.py"]