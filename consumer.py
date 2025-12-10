from confluent_kafka import Consumer, KafkaException
import json

# Replace with your public IP
conf = {
    'bootstrap.servers': '27.147.136.238:9092',
    'group.id': 'sensor_group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(**conf)
topic_name = 'sensor_data'
consumer.subscribe([topic_name])

print("Consuming messages from broker...")

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())
        data = json.loads(msg.value())
        print(f"Timestamp: {data['timestamp']}, Temperature: {data['temperature']}, Humidity: {data['humidity']}")
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
