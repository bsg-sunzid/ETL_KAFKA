import serial
import json
import time
import os
from confluent_kafka import Producer
from datetime import datetime

SERIAL_PORT = os.getenv("SERIAL_PORT", "/dev/ttyUSB0")
BAUD_RATE = 115200
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9093")
TOPIC = "sensor_data"

# Wait for Kafka to be ready
time.sleep(10)

producer_config = {
    'bootstrap.servers': KAFKA_BROKER,
    'client.id': 'sensor-producer'
}

producer = Producer(producer_config)

def delivery_report(err, msg):
    if err:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"✅ Delivered to {msg.topic()} [{msg.partition()}]")

print(f"Connecting to serial port: {SERIAL_PORT}")
ser = serial.Serial(SERIAL_PORT, BAUD_RATE, timeout=1)
time.sleep(2)

print("📡 Reading serial data and publishing to Kafka...")

try:
    while True:
        line = ser.readline().decode('utf-8', errors='ignore').strip()
        
        if "Publishing:" in line:
            try:
                json_part = line.split("Publishing:")[-1].strip()
                data = json.loads(json_part)

                msg = {
                    "timestamp": datetime.now().isoformat(),
                    "temperature": data["temp"],
                    "humidity": data["humidity"]
                }

                producer.produce(
                    TOPIC, 
                    json.dumps(msg).encode('utf-8'),
                    callback=delivery_report
                )
                producer.poll(0)
                
                print(f"📤 Sent: Temp={data['temp']}°C, Humidity={data['humidity']}%")
                
            except json.JSONDecodeError as e:
                print(f"⚠️ JSON decode error: {e}")
            except Exception as e:
                print(f"⚠️ Error: {e}")

except KeyboardInterrupt:
    print("\n🛑 Stopping producer...")
except Exception as e:
    print(f"❌ Fatal error: {e}")
finally:
    producer.flush()
    ser.close()
    print("👋 Producer stopped")
