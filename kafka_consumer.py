import json
from kafka import KafkaConsumer

# Kafka Configuration - UPDATED TO USE IP ADDRESS
KAFKA_BROKER = "192.168.0.108:9092"
KAFKA_TOPIC = "sensor-data"
GROUP_ID = "sensor-consumer-group"

# Create consumer
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    group_id=GROUP_ID,
    auto_offset_reset='earliest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print(f"🎯 Listening to Kafka topic: {KAFKA_TOPIC}")
print(f"🌐 Kafka Broker: {KAFKA_BROKER}")
print("=" * 60)

for message in consumer:
    data = message.value
    print(f"""
📊 New Sensor Reading:
   Timestamp: {data.get('timestamp')}
   Temperature: {data.get('temperature')}°C
   Humidity: {data.get('humidity')}%
   Topic: {data.get('topic')}
   Kafka Broker: {data.get('kafka_broker')}
   Partition: {message.partition}
   Offset: {message.offset}
""")
    print("=" * 60)