from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from confluent_kafka import Consumer, KafkaException
import json
import os
from datetime import datetime
from threading import Thread
from collections import deque

app = FastAPI(title="Sensor Data API")

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")

# Store last 100 readings in memory
sensor_readings = deque(maxlen=100)
latest_reading = {}

def consume_kafka():
    """Background thread to consume Kafka messages"""
    conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': 'api_consumer_group',
        'auto.offset.reset': 'latest',
        'enable.auto.commit': True
    }
    
    consumer = Consumer(**conf)
    consumer.subscribe(['sensor_data'])
    
    print("🔌 API Consumer connected to Kafka")
    
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"❌ Consumer error: {msg.error()}")
                continue
            
            try:
                data = json.loads(msg.value().decode('utf-8'))
                sensor_readings.append(data)
                latest_reading.update(data)
                print(f"✅ Received: Temp={data['temperature']}°C, Humidity={data['humidity']}%")
            except Exception as e:
                print(f"⚠️ Error processing message: {e}")
                
    except Exception as e:
        print(f"❌ Kafka consumer error: {e}")
    finally:
        consumer.close()

# Start Kafka consumer in background thread
@app.on_event("startup")
async def startup_event():
    thread = Thread(target=consume_kafka, daemon=True)
    thread.start()

@app.get("/")
async def root():
    return {
        "message": "Sensor Data API",
        "endpoints": {
            "latest": "/api/latest",
            "history": "/api/history",
            "stats": "/api/stats"
        }
    }

@app.get("/api/latest")
async def get_latest():
    """Get the latest sensor reading"""
    if not latest_reading:
        return {"error": "No data available yet"}
    return latest_reading

@app.get("/api/history")
async def get_history(limit: int = 50):
    """Get historical sensor readings"""
    readings = list(sensor_readings)[-limit:]
    return {
        "count": len(readings),
        "data": readings
    }

@app.get("/api/stats")
async def get_stats():
    """Get statistics for sensor readings"""
    if not sensor_readings:
        return {"error": "No data available yet"}
    
    temps = [r['temperature'] for r in sensor_readings]
    humidities = [r['humidity'] for r in sensor_readings]
    
    return {
        "total_readings": len(sensor_readings),
        "temperature": {
            "current": latest_reading.get('temperature'),
            "min": min(temps),
            "max": max(temps),
            "avg": round(sum(temps) / len(temps), 2)
        },
        "humidity": {
            "current": latest_reading.get('humidity'),
            "min": min(humidities),
            "max": max(humidities),
            "avg": round(sum(humidities) / len(humidities), 2)
        },
        "last_updated": latest_reading.get('timestamp')
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)