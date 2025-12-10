FROM python:3.12-slim

# Install dependencies
RUN pip install confluent_kafka pyserial

# Set working directory
WORKDIR /app

# Copy producer script
COPY producer.py .

# Environment variables
ENV SERIAL_PORT=/dev/ttyUSB0
ENV KAFKA_BROKER=kafka:9093

# Run producer
CMD ["python", "producer.py"]
