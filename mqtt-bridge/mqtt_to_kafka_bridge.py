import json
import os
import time
import socket
from paho.mqtt import client as mqtt_client
from paho.mqtt.client import CallbackAPIVersion
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# ================= CONFIG =================
MQTT_BROKER = os.getenv('MQTT_BROKER', 'broker.mqttdashboard.com')
MQTT_PORT = int(os.getenv('MQTT_PORT', 1883))
MQTT_TOPIC = os.getenv('MQTT_TOPIC', 'optiify-fdd-data-receive-test-cluster-01')

# Generate unique client ID to prevent conflicts
CONTAINER_ID = socket.gethostname()
MQTT_CLIENT_ID = f"mqtt-kafka-bridge-{CONTAINER_ID}-{int(time.time())}"

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:29092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'sensor-data')

print("Optiify MQTT → Kafka Bridge Starting (Docker)")
print(f"MQTT Broker : {MQTT_BROKER}:{MQTT_PORT}")
print(f"MQTT Topic  : {MQTT_TOPIC}")
print(f"Kafka Broker: {KAFKA_BROKER}")
print(f"Kafka Topic : {KAFKA_TOPIC}")
print(f"Client ID   : {MQTT_CLIENT_ID}")

# ================= KAFKA PRODUCER =================
def wait_for_kafka():
    retries = 30
    for attempt in range(1, retries + 1):
        try:
            print(f"Connecting to Kafka (Attempt {attempt}/{retries})")
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                linger_ms=10,
                acks='all',
                retries=3
            )
            print("✅ Kafka Producer connected")
            return producer
        except NoBrokersAvailable:
            if attempt < retries:
                print("⚠️  Kafka not ready, retrying in 2s...")
                time.sleep(2)
            else:
                raise RuntimeError("❌ Kafka connection failed")
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
            time.sleep(2)

producer = wait_for_kafka()

# ================= MQTT CALLBACKS =================
def on_connect(client, userdata, flags, reason_code, properties):
    """Callback for MQTT connection"""
    if reason_code == 0:
        print("✅ Connected to MQTT broker")
        client.subscribe(MQTT_TOPIC)
        print(f"📡 Subscribed to topic: {MQTT_TOPIC}")
    else:
        print(f"❌ MQTT connection failed: {reason_code}")

def on_message(client, userdata, msg):
    try:
        # Parse incoming JSON payload
        payload = json.loads(msg.payload.decode('utf-8'))
        
        # Add ingestion timestamp and metadata
        enriched_data = {
            "ingestion_timestamp": datetime.utcnow().isoformat() + "Z",
            "mqtt_topic": msg.topic,
            "source": "mqtt-bridge-docker",
            "bridge_client_id": MQTT_CLIENT_ID
        }
        
        # Merge original payload with enriched metadata
        # This preserves ALL fields from ESP32 without needing to know the format
        enriched_data.update(payload)
        
        # Send to Kafka
        future = producer.send(KAFKA_TOPIC, value=enriched_data)
        record_metadata = future.get(timeout=10)
        
        print(f"✅ Sent to Kafka [Partition: {record_metadata.partition}, Offset: {record_metadata.offset}]")
        
    except json.JSONDecodeError as e:
        print(f"❌ JSON decode error: {e}")
        print(f"   Raw payload: {msg.payload}")
    except Exception as e:
        print(f"❌ Error processing message: {e}")
        import traceback
        traceback.print_exc()

def on_disconnect(client, userdata, flags, reason_code, properties):
    """Callback for MQTT disconnection"""
    if reason_code != 0:
        print(f"⚠️  MQTT disconnected (code: {reason_code})")
        print("🔄 Automatic reconnection will be attempted...")

def on_subscribe(client, userdata, mid, reason_code_list, properties):
    """Callback for subscription acknowledgment"""
    print(f"✅ Subscription confirmed (mid: {mid})")

# ================= MQTT CLIENT =================
client = mqtt_client.Client(
    client_id=MQTT_CLIENT_ID,
    callback_api_version=CallbackAPIVersion.VERSION2,
    clean_session=True
)

client.on_connect = on_connect
client.on_message = on_message
client.on_disconnect = on_disconnect
client.on_subscribe = on_subscribe

# Enable automatic reconnection
client.reconnect_delay_set(min_delay=1, max_delay=120)

# ================= RUN =================
try:
    print(f"🔄 Connecting to MQTT broker {MQTT_BROKER}...")
    client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
    print("✅ MQTT connection initiated")
    print("🎯 Bridge is running and listening for messages...")
    client.loop_forever()

except KeyboardInterrupt:
    print("\n🛑 Bridge stopped by user")
except Exception as e:
    print(f"❌ Fatal error: {e}")
    import traceback
    traceback.print_exc()
finally:
    client.disconnect()
    producer.close()
    print("✅ Clean shutdown")