#!/bin/bash
echo "Waiting for schema registry to start listening"
while [ $(curl -s -o /dev/null -w %{http_code} $SCHEMA_REGISTRY_URL) -eq 000 ]
do
  echo -e $(date) " Schema registry listener HTTP state: " $(curl -s -o /dev/null -w %{http_code} $SCHEMA_REGISTRY_URL) " (waiting for 200)"
  sleep 5
done
echo "Schema registry is ready"
nc -vz $SCHEMA_REGISTRY_HOST $SCHEMA_REGISTRY_PORT
python /usr/local/bin/producer.py