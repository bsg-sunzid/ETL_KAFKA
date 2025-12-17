import json
import os
import time
from paho.mqtt import client as mqtt_client
from paho.mqtt.client import CallbackAPIVersion
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from datetime import datetime

# Get configuration from environment variables
MQTT_BROKER = os.getenv('MQTT_BROKER', 'broker.mqttdashboard.com')
MQTT_PORT = int(os.getenv('MQTT_PORT', 1883))
MQTT_TOPIC = os.getenv('MQTT_TOPIC', 'wokwi-weather')
MQTT_CLIENT_ID = "mqtt-kafka-bridge-docker"

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:29092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'sensor-data')

print("="*60)
print("🚀 MQTT to Kafka Bridge Starting...")
print(f"📡 MQTT Broker: {MQTT_BROKER}:{MQTT_PORT}")
print(f"📡 MQTT Topic: {MQTT_TOPIC}")
print(f"🔗 Kafka Broker: {KAFKA_BROKER}")
print(f"🔗 Kafka Topic: {KAFKA_TOPIC}")
print("="*60)

# Wait for Kafka to be ready
def wait_for_kafka():
    max_retries = 30
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            print(f"⏳ Attempting to connect to Kafka... (Attempt {retry_count + 1}/{max_retries})")
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                api_version=(0, 10, 1)
            )
            print("✅ Successfully connected to Kafka!")
            return producer
        except NoBrokersAvailable:
            retry_count += 1
            if retry_count < max_retries:
                print(f"❌ Kafka not ready yet, retrying in 2 seconds...")
                time.sleep(2)
            else:
                print("❌ Failed to connect to Kafka after all retries")
                raise
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
            retry_count += 1
            time.sleep(2)

# Initialize Kafka Producer
producer = wait_for_kafka()

# MQTT Callbacks - UPDATED for paho-mqtt 2.0
def on_connect(client, userdata, flags, reason_code, properties):
    """Callback for MQTT connection - Updated for v2.0"""
    if reason_code == 0:
        print(f"✅ Connected to MQTT Broker: {MQTT_BROKER}")
        client.subscribe(MQTT_TOPIC)
        print(f"📡 Subscribed to MQTT topic: {MQTT_TOPIC}")
    else:
        print(f"❌ Failed to connect to MQTT, reason code {reason_code}")

def on_message(client, userdata, msg):
    """Callback for received MQTT messages"""
    try:
        # Parse incoming MQTT message
        payload = json.loads(msg.payload.decode())
        
        # Add timestamp and metadata
        enriched_data = {
            "timestamp": datetime.utcnow().isoformat(),
            "topic": msg.topic,
            "temperature": payload.get("temp"),
            "humidity": payload.get("humidity"),
            "source": "mqtt-bridge-docker"
        }
        
        print(f"📥 Received from MQTT: Temp={enriched_data['temperature']}°C, Humidity={enriched_data['humidity']}%")
        
        # Send to Kafka
        future = producer.send(KAFKA_TOPIC, value=enriched_data)
        future.get(timeout=10)  # Wait for confirmation
        
        print(f"✅ Sent to Kafka topic: {KAFKA_TOPIC}")
        
    except json.JSONDecodeError as e:
        print(f"❌ JSON decode error: {e}")
    except Exception as e:
        print(f"❌ Error processing message: {e}")

def on_disconnect(client, userdata, flags, reason_code, properties):
    """Callback for MQTT disconnection - Updated for v2.0"""
    if reason_code != 0:
        print(f"⚠️ Unexpected MQTT disconnection. Code: {reason_code}")
        print("🔄 Attempting to reconnect...")

# Create MQTT Client - UPDATED for paho-mqtt 2.0
client = mqtt_client.Client(
    client_id=MQTT_CLIENT_ID,
    callback_api_version=CallbackAPIVersion.VERSION2
)
client.on_connect = on_connect
client.on_message = on_message
client.on_disconnect = on_disconnect

# Connect to MQTT
print(f"🔄 Connecting to MQTT broker {MQTT_BROKER}...")
try:
    client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
    print("✅ MQTT connection initiated")
    
    # Start the loop
    print("🎯 Bridge is now running and listening for messages...")
    print("="*60)
    client.loop_forever()
    
except KeyboardInterrupt:
    print("\n⚠️ Bridge stopped by user")
    client.disconnect()
    producer.close()
    print("✅ Disconnected gracefully")
except Exception as e:
    print(f"❌ Failed to connect to MQTT broker: {e}")
    raise