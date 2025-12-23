import json
from kafka import KafkaConsumer

# ================= CONFIGURATION =================
KAFKA_BROKER = "172.31.20.9:9092"
KAFKA_TOPIC = "sensor-data"
GROUP_ID = "sensor-consumer-simple"

print("🚀 Simple Kafka Consumer - Shows ALL Fields")
print(f"🌐 Broker: {KAFKA_BROKER} | Topic: {KAFKA_TOPIC}")
print("=" * 70)

# ================= CREATE CONSUMER =================
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    group_id=GROUP_ID,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("✅ Connected | Listening for any JSON format...\n")

# ================= CONSUME MESSAGES =================
message_count = 0

try:
    for message in consumer:
        message_count += 1
        
        print("=" * 70)
        print(f"📊 Message #{message_count} | Partition: {message.partition} | Offset: {message.offset}")
        print("=" * 70)
        
        # Pretty print ALL fields automatically
        print(json.dumps(message.value, indent=2, ensure_ascii=False))
        
        print("=" * 70)
        print()

except KeyboardInterrupt:
    print(f"\n🛑 Stopped | Total: {message_count} messages")
except Exception as e:
    print(f"❌ Error: {e}")
finally:
    consumer.close()