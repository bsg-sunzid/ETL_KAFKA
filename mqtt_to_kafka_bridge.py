import json
import time
from paho.mqtt import client as mqtt_client
from paho.mqtt.client import CallbackAPIVersion
from kafka import KafkaProducer
from datetime import datetime

# MQTT Configuration
MQTT_BROKER = "broker.mqttdashboard.com"
MQTT_PORT = 1883
MQTT_TOPIC = "wokwi-weather"
MQTT_CLIENT_ID = "mqtt-kafka-bridge"

# Kafka Configuration
KAFKA_BROKER = "192.168.0.108:9092"
KAFKA_TOPIC = "sensor-data"

# Initialize Kafka Producer
print("🔧 Initializing Kafka Producer...")
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    api_version=(0, 10, 1)
)
print(f"✅ Kafka Producer connected to {KAFKA_BROKER}")

def on_connect(client, userdata, flags, reason_code, properties):
    """Callback for when the client connects to the MQTT broker"""
    if reason_code == 0:
        print(f"✅ Connected to MQTT Broker: {MQTT_BROKER}")
        client.subscribe(MQTT_TOPIC)
        print(f"📡 Subscribed to topic: {MQTT_TOPIC}")
    else:
        print(f"❌ Failed to connect, reason code: {reason_code}")

def on_message(client, userdata, msg):
    """Callback for when a message is received from MQTT"""
    try:
        # Parse incoming MQTT message
        payload = json.loads(msg.payload.decode())
        
        # Add timestamp and source IP
        enriched_data = {
            "timestamp": datetime.utcnow().isoformat(),
            "topic": msg.topic,
            "temperature": payload.get("temp"),
            "humidity": payload.get("humidity"),
            "kafka_broker": KAFKA_BROKER
        }
        
        print(f"\n📥 Received from MQTT:")
        print(f"   Temperature: {enriched_data['temperature']}°C")
        print(f"   Humidity: {enriched_data['humidity']}%")
        print(f"   Timestamp: {enriched_data['timestamp']}")
        
        # Send to Kafka
        future = producer.send(KAFKA_TOPIC, value=enriched_data)
        producer.flush()
        
        # Get metadata from send result
        record_metadata = future.get(timeout=10)
        
        print(f"✅ Sent to Kafka:")
        print(f"   Topic: {record_metadata.topic}")
        print(f"   Partition: {record_metadata.partition}")
        print(f"   Offset: {record_metadata.offset}")
        print("=" * 60)
        
    except json.JSONDecodeError as e:
        print(f"❌ JSON decode error: {e}")
    except Exception as e:
        print(f"❌ Error processing message: {e}")

def on_disconnect(client, userdata, flags, reason_code, properties):
    """Callback for when the client disconnects"""
    print(f"⚠️  Disconnected from MQTT broker, reason code: {reason_code}")
    if reason_code != 0:
        print("🔄 Unexpected disconnection. Reconnecting...")

def on_subscribe(client, userdata, mid, reason_code_list, properties):
    """Callback for when subscription is confirmed"""
    print(f"✅ Subscription confirmed (Message ID: {mid})")

# Create MQTT Client with CallbackAPIVersion
print("🔧 Initializing MQTT Client...")
client = mqtt_client.Client(
    client_id=MQTT_CLIENT_ID,
    callback_api_version=CallbackAPIVersion.VERSION2
)

# Set callbacks
client.on_connect = on_connect
client.on_message = on_message
client.on_disconnect = on_disconnect
client.on_subscribe = on_subscribe

# Connect and loop
print(f"🔄 Connecting to MQTT broker: {MQTT_BROKER}:{MQTT_PORT}")
try:
    client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
    print("🎯 Starting MQTT loop... (Press Ctrl+C to stop)")
    print("=" * 60)
    client.loop_forever()
except KeyboardInterrupt:
    print("\n\n⚠️  Interrupted by user")
    client.disconnect()
    producer.close()
    print("✅ Disconnected gracefully")
except Exception as e:
    print(f"❌ Connection error: {e}")
    producer.close()