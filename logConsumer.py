import gzip
import json
from kafka import KafkaConsumer

# Configure the Kafka consumer
consumer = KafkaConsumer(
    'processed_logs',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda v: v,  # We'll handle deserialization after decompression
    auto_offset_reset='earliest',
    enable_auto_commit=True
)

def consume_logs():
    try:
        for message in consumer:
            headers = dict(message.headers or [])
            content_encoding = headers.get('Content-Encoding'.encode('utf-8'))

            if content_encoding == b'gzip':
                decompressed_value = gzip.decompress(message.value)
                log = json.loads(decompressed_value.decode('utf-8'))
                print(f"[{log['level']}] {log['timestamp']} - {log['service']}: {log['message']}")
            else:
                print("Received uncompressed message or unknown encoding.")
    except KeyboardInterrupt:
        print("Consumer stopped.")
    finally:
        consumer.close()

if __name__ == "__main__":
    consume_logs()
