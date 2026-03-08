# Test Consumer for SolarXAI

This test consumer reads messages from all Kafka topics and saves them to files for verification purposes.

## How to Run the Test Consumer

### Basic Usage (Recommended)

```bash
cd /home/paupaf3/git-repos/solarxai/test-consumer
python3 test_consumer.py
```

This will:
- Auto-discover all available Kafka topics
- Connect to all three brokers (localhost:9092, localhost:9192, localhost:9292)
- Save messages to `./output/` directory
- Create one file per topic: `{topic_name}_messages.jsonl`

### Check Available Topics First

```bash
python3 test_consumer.py --list-topics
```

### Custom Usage Examples

```bash
# Use specific brokers
python3 test_consumer.py --brokers localhost:9092,localhost:9192

# Use specific topics
python3 test_consumer.py --topics on_site_meteo_station,inverter

# Use custom output directory
python3 test_consumer.py --output-dir /tmp/kafka-test

# Combine options
python3 test_consumer.py --brokers localhost:9092 --topics inverter --output-dir ./inverter-data
```

## Verification Steps

### 1. Test Kafka Connectivity

First, test if you can connect to Kafka:

```bash
python3 -c "
from kafka import KafkaConsumer
import sys
try:
    consumer = KafkaConsumer(bootstrap_servers=['localhost:9092', 'localhost:9192', 'localhost:9292'], consumer_timeout_ms=5000)
    topics = consumer.topics()
    consumer.close()
    print(f'✓ Connected successfully. Found {len(topics)} topics: {sorted(topics)}')
except Exception as e:
    print(f'✗ Connection failed: {e}')
    sys.exit(1)
"
```

### 2. Start the Consumer

```bash
python3 test_consumer.py
```

You should see output like:
```
Test Consumer initialized:
  Brokers: ['localhost:9092', 'localhost:9192', 'localhost:9292']
  Output directory: output
  Topics to consume: ['on_site_meteo_station', 'inverter', 'poi_meter', 'system_alerts', 'processed_data']
Created consumer for topic: on_site_meteo_station
Created output file: output/on_site_meteo_station_messages.jsonl
Starting consumer thread for topic: on_site_meteo_station
...
```

### 3. Start Data Producers

In another terminal, start the data simulator:

```bash
cd /home/paupaf3/git-repos/solarxai
./start-solarxai.sh
```

### 4. Verify Messages Are Being Received

The consumer will print messages as they arrive:
```
[on_site_meteo_station] Saved message at 2025-10-02T10:30:15.123456
[inverter] Saved message at 2025-10-02T10:30:16.234567
[poi_meter] Saved message at 2025-10-02T10:30:17.345678
```

### 5. Check Output Files

```bash
# List generated files
ls -la output/

# Check file contents (shows last 5 messages)
tail -5 output/on_site_meteo_station_messages.jsonl

# Count messages per topic
wc -l output/*.jsonl

# View a specific message (pretty printed)
tail -1 output/inverter_messages.jsonl | python3 -m json.tool
```

## File Format

Each message is saved as a JSON line in the format:
```json
{
  "timestamp": "2025-10-02T10:30:15.123456",
  "topic": "on_site_meteo_station",
  "partition": 0,
  "offset": 12345,
  "data": {
    "partition": 0,
    "offset": 12345,
    "key": null,
    "value": {
      "guid": "some-guid",
      "temperature": 25.5,
      "humidity": 60.2,
      "...": "actual message data"
    },
    "timestamp": 1696248615123,
    "timestamp_type": 0
  }
}
```

## Stopping the Consumer

Press `Ctrl+C` to gracefully stop the consumer. It will:
- Close all Kafka connections
- Close all file handles
- Print shutdown confirmation