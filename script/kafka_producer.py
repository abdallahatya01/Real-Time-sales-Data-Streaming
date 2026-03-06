import json
import time
import uuid
import random
import logging
from datetime import datetime
from kafka import KafkaProducer


logging.basicConfig(level=logging.INFO)

KAFKA_TOPIC = "sales_stream"
KAFKA_BROKER = "broker:29092"

countries = ["Egypt", "UAE", "KSA", "Germany", "USA"]
products = ["Laptop", "Phone", "Tablet", "Monitor", "Keyboard"]
payment_methods = ["card", "cash", "paypal"]


def generate_sale():

    quantity = random.randint(1, 5)
    price = round(random.uniform(100, 2000), 2)

    return {
        "order_id": str(uuid.uuid4()),
        "customer_id": random.randint(1000, 5000),
        "product": random.choice(products),
        "category": "electronics",
        "price": price,
        "quantity": quantity,
        "total_amount": round(price * quantity, 2),
        "country": random.choice(countries),
        "payment_method": random.choice(payment_methods),
        "order_time": datetime.utcnow().isoformat()
    }


def start_stream():

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    logging.info("Starting sales data stream...")

    while True:

        sale = generate_sale()

        try:
            producer.send(KAFKA_TOPIC, sale)
            logging.info(f"Sent event: {sale['order_id']}")
        except Exception as e:
            logging.error(f"Kafka error: {e}")

        time.sleep(10)


if __name__ == "__main__":
    start_stream()