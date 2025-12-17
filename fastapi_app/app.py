import json
import asyncio
import threading
from datetime import datetime
from fastapi import FastAPI
from confluent_kafka import Producer
import paho.mqtt.client as mqtt

app = FastAPI()
sensor_data = []

# ----------------- Kafka Producer -----------------
producer = Producer({'bootstrap.servers': 'kafka:9092'})

def send_to_kafka(reading):
    producer.produce('sensor_data', json.dumps(reading).encode('utf-8'))
    producer.flush()

# ----------------- MQTT Subscriber -----------------
MQTT_BROKER = "mosquitto"
MQTT_PORT = 1883
MQTT_TOPIC = "wokwi-weather"

def on_connect(client, userdata, flags, rc):
    print(f"Connected to MQTT broker with code {rc}")
    client.subscribe(MQTT_TOPIC)

def on_message(client, userdata, msg):
    try:
        data = json.loads(msg.payload.decode())
        data["timestamp"] = datetime.now().isoformat()
        sensor_data.append(data)
        print(f"MQTT Received: {data}")
        send_to_kafka(data)
    except Exception as e:
        print(f"Error decoding MQTT message: {e}")

mqtt_client = mqtt.Client()
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message

def mqtt_thread():
    mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
    mqtt_client.loop_forever()

threading.Thread(target=mqtt_thread, daemon=True).start()

# ----------------- FastAPI Endpoints -----------------
@app.get("/sensor")
async def get_all_readings():
    return sensor_data

@app.get("/stream")
async def stream():
    async def event_generator():
        last_index = 0
        while True:
            if len(sensor_data) > last_index:
                for item in sensor_data[last_index:]:
                    yield f"data: {json.dumps(item)}\n\n"
                last_index = len(sensor_data)
            await asyncio.sleep(1)
    return app.responses.StreamingResponse(event_generator(), media_type="text/event-stream")
