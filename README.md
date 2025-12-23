# ETL_KAFKA

## 🔹 Features

1. **MQTT → Kafka Bridge**
   - Subscribes to MQTT topics.
   - Enriches payload with metadata and timestamps.
   - Produces messages to Kafka.

2. **Kafka Consumer**
   - Consumes messages from Kafka in JSON format.
   - Prints all fields in a pretty JSON format.

3. **Report Generation**
   - Converts consumed Kafka messages into CSV format for reporting.

---

## 🔹 Prerequisites

- Docker & Docker Compose  
- Python 3.10+  
- Network access to MQTT broker

---

## 🔹 Python Dependencies

Install required Python packages:

```bash
pip install -r requirements.txt
````

Packages included:

* `paho-mqtt` → MQTT client
* `kafka-python` → Kafka producer & consumer
* `python-dotenv` → Load environment variables from `.env`

---

## 🔹 Docker Setup

1. **Update IP address in `docker-compose.yaml`**

```yaml
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://<YOUR_HOST_IP>:9092,PLAINTEXT_INTERNAL://kafka:29092
```

> Replace `<YOUR_HOST_IP>` with your host machine IP so external consumers can connect.

2. Start services:

```bash
docker compose build --no-cache mqtt-kafka-bridge
docker compose up -d
docker compose logs -f mqtt-kafka-bridge
```

This will start:

* Zookeeper
* Kafka broker
* MQTT → Kafka bridge
* Kafka UI (web interface)

---

## 🔹 Python Bridge Configuration

Update `.env` or environment variables in `mqtt-kafka-bridge`:

```env
MQTT_BROKER=broker.mqttdashboard.com
MQTT_PORT=1883
MQTT_TOPIC=optiify-fdd-data-receive-test-cluster-01
KAFKA_BROKER=kafka:29092
KAFKA_TOPIC=sensor-data
```

---

## 🔹 Kafka Consumer

In `kafka_consumer.py`, ensure the **broker IP matches** the `KAFKA_ADVERTISED_LISTENERS` host IP:

```python
KAFKA_BROKER = "<YOUR_HOST_IP>:9092"
```

Run the consumer:

```bash
python kafka_consumer.py
```

* Output: JSON formatted messages.
* Pretty-printed for readability.

---

## 🔹 Generate Report

Run `report.py` to convert Kafka messages to CSV:

```bash
python report.py
```

* Output: CSV file with all consumed message fields.
---

## 🔹 Kafka UI (Optional)

* Access via browser at: `http://localhost:8080`
* Kafka UI connects to internal Kafka listener: `kafka:29092`
* View topics, messages, partitions, and offsets.

---