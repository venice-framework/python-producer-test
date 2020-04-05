This producer simulates location updates (latitude and longitude) from a moving bus.

The producer used in the image is `producer.py`, which uses `AvroProducer`. It contains a workaround in order to be compatible with KSQL: it sends the bus_id in the value as well as setting it as the key. 

An alternative producer is `producer_key_str.py`, which uses a generic `Producer`, and encodes the key as string, and value as Avro.

If you want to modify the `producer.py` file and test out your changes in a venice pipeline, you can run `docker build --tag YOUR_IMAGE_NAME.` in this repo, then use `image: YOUR_IMAGE_NAME` in your `docker-compose.yml` file.

If you are making many changes and you want docker-compose to build your image every time you run `docker-compose up --build`, you can use `build: PATH_TO_THIS_REPO` in your `docker-compose.yml` file.

If you want to save your image and push to an image registry, you can use `YOUR_REGISTRY_ACCOUNT/YOUR_IMAGE_NAME` to have the image tagged as `latest` by default, or `YOUR_REGISTRY_ACCOUNT/YOUR_IMAGE_NAME:YOUR_TAG` if you want to specify a tag.

Reference: https://github.com/confluentinc/confluent-kafka-python