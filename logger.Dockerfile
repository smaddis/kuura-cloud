FROM ubuntu:20.04
RUN apt-get update -y && apt-get install -y python3 python3-pip
WORKDIR /app
COPY ./logger/* .
RUN pip3 install -r ./requirements.txt
ENTRYPOINT [ "python3" ]
CMD [ "mqtt_logger.py" ]

