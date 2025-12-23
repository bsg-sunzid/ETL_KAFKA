import json
import csv
import os
from kafka import KafkaConsumer
from datetime import datetime
from collections import OrderedDict

# ================= CONFIGURATION =================
KAFKA_BROKER = "172.31.20.9:9092"
KAFKA_TOPIC = "sensor-data"
GROUP_ID = "sensor-consumer-csv-export"

CSV_FILE = "sensor_data_output.csv"

print("Optiify Kafka Consumer (Dynamic CSV Export)")
print(f"Broker   : {KAFKA_BROKER}")
print(f"Topic    : {KAFKA_TOPIC}")
print(f"Group ID : {GROUP_ID}")
print(f"CSV File : {CSV_FILE}")

# ================= DYNAMIC CSV SETUP =================

# Track all unique fields encountered
all_fields = OrderedDict()
csv_file = None
csv_writer = None

def flatten_json(data, parent_key='', sep='_'):
    items = []
    for key, value in data.items():
        new_key = f"{parent_key}{sep}{key}" if parent_key else key
        
        if isinstance(value, dict):
            items.extend(flatten_json(value, new_key, sep=sep).items())
        elif isinstance(value, list):
            # Convert list to JSON string
            items.append((new_key, json.dumps(value, ensure_ascii=False)))
        else:
            items.append((new_key, value))
    
    return dict(items)

def update_csv_structure(new_fields):
    global all_fields, csv_file, csv_writer
    
    # Check if we have new fields
    new_field_detected = False
    for field in new_fields:
        if field not in all_fields:
            all_fields[field] = None
            new_field_detected = True
    
    if not new_field_detected and csv_writer is not None:
        return 
    
    # If new fields found, we need to recreate the CSV with updated headers
    if csv_file is not None:
        csv_file.close()
    
    # Read existing data if file exists
    existing_data = []
    if os.path.exists(CSV_FILE):
        try:
            with open(CSV_FILE, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                existing_data = list(reader)
        except Exception as e:
            print(f"⚠️  Warning: Could not read existing CSV: {e}")
    
    # Create new file with updated headers
    csv_file = open(CSV_FILE, mode='w', newline='', encoding='utf-8')
    
    # Define field order: metadata fields first, then data fields
    metadata_priority = [
        'consumer_timestamp', 'ingestion_timestamp', 'timestamp',
        'kafka_partition', 'kafka_offset'
    ]
    
    # Order fields: priority metadata first, then alphabetically
    ordered_fields = []
    for field in metadata_priority:
        if field in all_fields:
            ordered_fields.append(field)
    
    for field in sorted(all_fields.keys()):
        if field not in ordered_fields:
            ordered_fields.append(field)
    
    csv_writer = csv.DictWriter(csv_file, fieldnames=ordered_fields, extrasaction='ignore')
    csv_writer.writeheader()
    
    # Write back existing data with new fields
    if existing_data:
        for row in existing_data:
            # Fill missing fields with empty values
            for field in ordered_fields:
                if field not in row:
                    row[field] = ''
            csv_writer.writerow(row)
        print(f"✅ CSV restructured: {len(existing_data)} existing rows preserved")
    
    if new_field_detected:
        print(f"🆕 New fields detected: {', '.join([f for f in new_fields if f not in metadata_priority])}")

# ================= CREATE CONSUMER =================
try:
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8"))
    )
    print("✅ Consumer connected successfully")
    print("🎯 Listening for messages (any format)...\n")
except Exception as e:
    print(f"❌ Failed to connect to Kafka: {e}")
    exit(1)

# ================= CONSUME MESSAGES =================
message_count = 0

try:
    for message in consumer:
        message_count += 1
        data = message.value
        
        # Flatten nested JSON structures
        flattened_data = flatten_json(data)
        
        # Add Kafka metadata
        flattened_data['kafka_partition'] = message.partition
        flattened_data['kafka_offset'] = message.offset
        flattened_data['consumer_timestamp'] = datetime.utcnow().isoformat() + 'Z'
        
        # Update CSV structure if new fields found
        update_csv_structure(flattened_data.keys())
        
        # Write row to CSV
        csv_writer.writerow(flattened_data)
        csv_file.flush()
        
        # Console output - show key fields
        console_info = f"📊 #{message_count} | Offset={message.offset}"
        
        # Try to show meaningful identifiers
        if 'device_id' in flattened_data:
            console_info += f" | Device={flattened_data['device_id']}"
        if 'temperature' in flattened_data:
            console_info += f" | Temp={flattened_data['temperature']}°C"
        if 'humidity' in flattened_data:
            console_info += f" | Humidity={flattened_data['humidity']}%"
        
        # Show first data field if no standard fields found
        if 'device_id' not in flattened_data:
            data_fields = [k for k in flattened_data.keys() if k not in 
                          {'kafka_partition', 'kafka_offset', 'consumer_timestamp', 
                           'ingestion_timestamp', 'mqtt_topic', 'source', 'bridge_client_id'}]
            if data_fields:
                first_field = data_fields[0]
                console_info += f" | {first_field}={flattened_data[first_field]}"
        
        print(console_info)
        
        # Print field summary every 50 messages
        if message_count % 50 == 0:
            print(f"\n📈 Stats: {message_count} messages | {len(all_fields)} unique fields\n")

except KeyboardInterrupt:
    print(f"Consumer stopped by user")
    print(f"Total messages processed: {message_count}")
    print(f"Total unique fields: {len(all_fields)}")
    print(f"Data saved to: {CSV_FILE}")

except Exception as e:
    print(f"\n❌ Consumer error: {e}")
    import traceback
    traceback.print_exc()

finally:
    consumer.close()
    if csv_file:
        csv_file.close()
    print("✅ Consumer and CSV file closed gracefully")