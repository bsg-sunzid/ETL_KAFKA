from confluent_kafka import Consumer
import asyncio
from fastapi import FastAPI
from fastapi.responses import StreamingResponse

conf = {
    'bootstrap.servers': 'kafka:9092',  # Docker service name
    'group.id': 'my_group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
consumer.subscribe(['my_topic'])

app = FastAPI()

async def kafka_stream():
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            await asyncio.sleep(0.1)
            continue
        if msg.error():
            continue
        yield f"{msg.value().decode('utf-8')}\n"

@app.get("/stream")
async def stream():
    return StreamingResponse(kafka_stream(), media_type="text/plain")
