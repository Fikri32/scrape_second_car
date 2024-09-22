from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

for i in range(10):
    message = {"test": i}
    print(f"Sending message: {message}")
    producer.send("oto-scrape", message)
    time.sleep(1)  # Introduce a delay for testing
producer.flush()
