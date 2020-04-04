# choice of python base image:
# https://pythonspeed.com/articles/base-image-python-docker-images/
FROM python:3.5-slim-buster
# REMINDER: you will need to run apt-get update
# if you want to apt-get install anything, because 
# there isn't a package cache in the image yet
RUN apt-get update && apt-get install apt-utils curl netcat -y
RUN pip install "confluent-kafka[avro]" requests
COPY docker-entrypoint.sh *.py usr/local/bin/
RUN ["chmod", "+x", "usr/local/bin/docker-entrypoint.sh"]
#ENTRYPOINT ["tail", "-f", "/dev/null"]
ENTRYPOINT ["docker-entrypoint.sh"]