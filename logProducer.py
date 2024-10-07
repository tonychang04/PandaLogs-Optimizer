import json
import time
import random
from kafka import KafkaProducer

# Configure the Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092', 
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Define possible log levels and services
log_levels = ['DEBUG', 'INFO', 'WARN', 'ERROR']
services = ['AuthService', 'PaymentService', 'UserService', 'OrderService']

def generate_log_message():
    log = {
        'timestamp': int(time.time() * 1000),
        'level': random.choice(log_levels),
        'service': random.choice(services),
        'message': 'Sample log message',
        'userId': random.randint(1000, 9999),
        'transactionId': random.randint(100000, 999999),
        'details': 'Additional information about the event.',
        'unnecessaryField': 'This field is unnecessary and will be removed.'
    }
    return log

def send_logs():
    try:
        while True:
            log_message = generate_log_message()
            # Send the log message to the 'raw_logs' topic
            producer.send('raw_logs', value=log_message)
            print(f"Produced log: {log_message}")
            time.sleep(0.1)  # Adjust the sleep time to control the log generation rate
    except KeyboardInterrupt:
        print("Log generation stopped.")
    finally:
        producer.close()

if __name__ == "__main__":
    send_logs()
