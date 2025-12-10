import serial
import csv
import time
import json
from datetime import datetime

# ---------------- Configuration ----------------
SERIAL_PORT = "/dev/ttyUSB0"  # Update to your ESP32 port
BAUD_RATE = 115200
CSV_FILE = "dht_readings.csv"
# ------------------------------------------------

def main():
    ser = serial.Serial(SERIAL_PORT, BAUD_RATE, timeout=1)
    time.sleep(2)  # Wait for ESP32 to initialize

    with open(CSV_FILE, mode='a+', newline='') as csvfile:
        csv_writer = csv.writer(csvfile)

        # Write header if file is empty
        csvfile.seek(0)
        if csvfile.read(1) == "":
            csv_writer.writerow(["datetime", "temperature", "humidity"])

        while True:
            try:
                line = ser.readline().decode('utf-8').strip()
                if not line:
                    continue

                # Check if line contains JSON data
                if "Publishing:" in line:
                    # Extract JSON part
                    json_part = line.split("Publishing:")[-1].strip()
                    data = json.loads(json_part)

                    # Extract temperature and humidity
                    temp = data.get("temp")
                    hum = data.get("humidity")

                    # Extract timestamp from serial line
                    timestamp_str = line.split("->")[0].strip()
                    # Convert to datetime
                    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

                    # Write to CSV
                    csv_writer.writerow([now, temp, hum])
                    csvfile.flush()
                    print(f"Saved: {now}, {temp}, {hum}")

            except KeyboardInterrupt:
                print("Exiting...")
                break
            except Exception as e:
                print("Error:", e)
                continue

if __name__ == "__main__":
    main()
