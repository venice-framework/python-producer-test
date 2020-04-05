from confluent_kafka import avro, Producer
from confluent_kafka.avro import CachedSchemaRegistryClient
from confluent_kafka.avro.serializer.message_serializer import MessageSerializer

import os
import requests
import time

from admin_api import CustomAdmin

"""
This is an example with fake data intended to demonstrate basic
functionality of the pipeline.

The context is a bus updating its location in physical space as
it moves.

The key is bus_id and the values are lat and lng.

Every event represents a location change.
The latitude and longitude are changes by a constant value
with every event.

You must set the TOPIC_NAME environment variable.
This image does not have a default TOPIC_NAME set, to avoid
potentially confusing errors.

References:
https://github.com/confluentinc/confluent-kafka-python
https://github.com/confluentinc/confluent-kafka-python/issues/428#issuecomment-444320103
https://docs.confluent.io/current/clients/confluent-kafka-python/

"""

BROKER = os.environ['BROKER']
SCHEMA_REGISTRY_URL = os.environ['SCHEMA_REGISTRY_URL']
TOPIC_NAME = os.environ['TOPIC_NAME']


def delivery_callback(err, msg):
    """
    Called once for each message produced to indicate delivery result.
    Triggered by poll() or flush().
    """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.
              format(msg.topic(), msg.partition()))


# Create a topic if it doesn't exist yet
print('Checking if topic: {} exists...'.format(TOPIC_NAME))
admin = CustomAdmin(BROKER)
if not admin.topic_exists(TOPIC_NAME):
    admin.create_topics([TOPIC_NAME])

# Define wrapper function for serializing in avro format
serialize_avro = MessageSerializer(
    CachedSchemaRegistryClient(SCHEMA_REGISTRY_URL)
).encode_record_with_schema

# Define value schema
value_schema = avro.loads("""
    {
        "namespace": "septa.bus.location",
        "name": "value",
        "type": "record",
        "fields": [
            {"name": "lat", "type": "float", "doc": "latitude"},
            {"name": "lng", "type": "float", "doc": "longitude"}
        ]
    }
""")

# Initialize producer
# Configuration options:
# https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
producer = Producer(
    {
        'bootstrap.servers': BROKER,
        'default.topic.config': {'acks': 1}
    }
)

# Initialize key and values
key = 1
lat = 40.043152
lng = -75.18071

# Produce n events simulating bus movements
n = 10000
for i in range(n):
    value = {
        "lat": lat,
        "lng": lng
    }
    serialized_value = serialize_avro(topic=TOPIC_NAME,
                                      schema=value_schema,
                                      record=value,
                                      is_key=False)
    serialized_key = str(key)
    try:
        producer.produce(topic=TOPIC_NAME,
                         value=serialized_value,
                         key=serialized_key,
                         callback=delivery_callback)
    except BufferError:
        print("Local producer queue is full: {} messages in queue".format(
          len(producer)))

    print("EVENT COUNT: {} key: {} lat: {}, lng: {}".format(i, key, lat, lng))
    # Polls the producer for events and calls the corresponding callbacks
    # (if registered)
    #
    # `timeout` refers to the maximum time to block waiting for events
    #
    # Since produce() is an asynchronous API this poll() call will most
    # likely not serve the delivery callback for the last produce()d message.
    lat += 0.000001
    lng += 0.000001
    producer.poll(timeout=0)
# Cleanup step: wait for all messages to be delivered before exiting.
producer.flush()
