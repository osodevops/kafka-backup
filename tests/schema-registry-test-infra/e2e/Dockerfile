# Independent clients for the Schema Registry end-to-end test (issue #262):
# the official Confluent Python client with its Avro, JSON Schema and
# Protobuf serializers and deserializers.
FROM python:3.12-slim-bookworm
RUN pip install --no-cache-dir "confluent-kafka[avro,json,protobuf]==2.11.1" requests==2.32.5
WORKDIR /app
COPY clients.py .
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PYTHONWARNINGS=ignore
ENTRYPOINT ["python", "/app/clients.py"]
