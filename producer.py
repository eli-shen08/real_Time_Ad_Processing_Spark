from time import sleep
from uuid import uuid4, UUID
from dotenv import load_dotenv
import time
from datetime import datetime, timedelta

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
import random
import json
import os


def delivery_report(err, msg):
    """
    Reports the failure or success of a message delivery.

    Args:
        err (KafkaError): The error that occurred on None on success.

        msg (Message): The message that was produced or failed.
    """
    if err is not None:
        print(f"Delivery failed for User record {msg.key()}: {err}")
        return
    print(f"User record {msg.key()} successfully produced to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
    print("-"*100)


# Define Kafka configuration
load_dotenv()
kafka_id = os.environ.get("confluent_kafka_id")
kafka_secret_key = os.environ.get("confluent_kafka_secret_key")
kafka_bootstrap = os.environ.get("kafka_server")
kafka_config = {
    'bootstrap.servers': kafka_bootstrap,
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': kafka_id,
    'sasl.password': kafka_secret_key
}

# Create a Schema Registry client
url = os.environ.get("url")
schema_id = os.environ.get("confluence_schema_id")
schema_secret = os.environ.get("confluence_schema_secret")
schema_registry_client = SchemaRegistryClient({
  'url': url,
  'basic.auth.user.info': '{}:{}'.format(schema_id, schema_secret)
})


topic = 'ad_topic'
subject_name = f"{topic}-value"


# Fetch the latest schema dynamically
def get_latest_schema(subject):
    schema = schema_registry_client.get_latest_version(subject).schema.schema_str
    return AvroSerializer(schema_registry_client, schema)

key_serializer = StringSerializer('utf-8')

producer = SerializingProducer({**kafka_config,
                                'key.serializer':key_serializer,
                                'value.serializer':get_latest_schema(subject_name)})

def generate_order(ad_id: str) -> dict:
    return {
        "ad_id": str(ad_id),
        # Random timestamp within the last 30 minutes
        "timestamp": (datetime.utcnow() - timedelta(minutes=random.randint(0, 30))).isoformat() + "Z",
        "clicks": random.randint(1, 20),      # random number of clicks
        "views": random.randint(10, 200),     # random number of views
        "cost": round(random.uniform(1.0, 100.0), 2)  # random cost
    }

try:
    ad_id_counter = 64
    for _ in range(50):
        ad_id = ad_id_counter
        order = generate_order(ad_id)
    
        producer.produce(topic,
                        key=str(order['ad_id']),
                        value=order,
                        on_delivery=delivery_report)
        producer.poll(0)
        print("Sent order ->", order)
        # Random duplicate
        if random.choice([True, False]):
            producer.produce(topic,
                            key=str(order['ad_id']),
                            value=order,
                            on_delivery=delivery_report)
            producer.poll(0)
            print("Sent duplicate order ->", order)

        ad_id_counter += 1
        time.sleep(1)
finally:
    producer.flush()
    print('Successfully published mock data')    


