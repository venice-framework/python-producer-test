# Choice of python base image:
# https://pythonspeed.com/articles/base-image-python-docker-images/
FROM python:3.5-slim-buster
RUN apt-get update \
    && apt-get install apt-utils curl netcat -y \
    && pip install "confluent-kafka[avro]" requests
ENV BROKER="broker-1:9092" \
    SCHEMA_REGISTRY_HOST="schema-registry" \
    SCHEMA_REGISTRY_PORT=8081 \
    SCHEMA_REGISTRY_URL="http://schema-registry:8081"
COPY docker-entrypoint.sh *.py usr/local/bin/
RUN ["chmod", "+x", "usr/local/bin/docker-entrypoint.sh"]
ENTRYPOINT ["docker-entrypoint.sh"]