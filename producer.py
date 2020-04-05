from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer

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

This version of the producer serializes the key with avro.

For a version that serializes the key as a string, see the repo.
"""

BROKER = os.environ['BROKER']
SCHEMA_REGISTRY_URL = os.environ['SCHEMA_REGISTRY_URL']
TOPIC_NAME = os.environ['TOPIC_NAME']


def delivery_report(err, msg):
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
admin = CustomAdmin(BROKER)
if not admin.topic_exists(TOPIC_NAME):
    admin.create_topics([TOPIC_NAME])

# Define schemas
# NOTE: bus_id is included in the value as a hacky workaround
# because ksql does not recognize avro-encoded keys and
# AvroProducer does not allow a different encoding for keys at the
# time of this writing.
# See https://github.com/confluentinc/confluent-kafka-python/issues/428
value_schema = avro.loads("""
    {
        "namespace": "septa.bus.location",
        "name": "value",
        "type": "record",
        "fields": [
            {"name": "bus_id", "type": "int", "doc": "bus id"},
            {"name": "lat", "type": "float", "doc": "latitude"},
            {"name": "lng", "type": "float", "doc": "longitude"}
        ]
    }
""")

key_schema = avro.loads("""
{
   "namespace": "septa.bus.location",
   "name": "key",
   "type": "record",
   "fields" : [
     {
       "name" : "bus_id",
       "type" : "int"
     }
   ]
}
""")

# Initialize producer
avroProducer = AvroProducer(
    {
        'bootstrap.servers': BROKER,
        'on_delivery': delivery_report,
        'schema.registry.url': SCHEMA_REGISTRY_URL
    },
    default_key_schema=key_schema,
    default_value_schema=value_schema
)

# Initialize key and values
lat = 40.043152
lng = -75.18071
bus_id = 1

key = {"bus_id": 1}

# Produce events simulating bus movements, forever
count = 1
while True:
    value = {
        "bus_id": bus_id,
        "lat": lat,
        "lng": lng
    }
    avroProducer.produce(topic=TOPIC_NAME, value=value, key=key)
    print("EVENT COUNT: {} key: {} lat: {}, lng: {}".format(count, key, lat, lng))
    # Polls the producer for events and calls the corresponding callbacks
    # (if registered)
    #
    # `timeout` refers to the maximum time to block waiting for events
    #
    # Since produce() is an asynchronous API this poll() call will most
    # likely not serve the delivery callback for the last produce()d message.
    avroProducer.poll(timeout=0)
    time.sleep(0.3)

    lat += 0.000001
    lng += 0.000001
    count += 1
# Cleanup step: wait for all messages to be delivered before exiting.
avroProducer.flush()
