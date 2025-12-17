FROM python:3.9-slim

WORKDIR /app

# Install dependencies
RUN pip install --no-cache-dir paho-mqtt kafka-python

# Copy bridge script
COPY mqtt_to_kafka_bridge.py .

# Run the bridge
CMD ["python", "-u", "mqtt_to_kafka_bridge.py"]