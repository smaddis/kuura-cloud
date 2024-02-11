import datetime
import json
import logging
import os
import time

from typing import Tuple

import yaml

import paho.mqtt.client as mqtt

from influxdb_client import WritePrecision, InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

_logger = logging.getLogger(__name__)

try:
    _BROKER_URL = os.environ['MQTT_URL']
    _BROKER_PORT = os.environ['MQTT_PORT']
    _CERT_PATH = os.environ['CERT_PATH']
    _CLIENT_ID = os.environ['CLIENT_ID']
    _CLIENT_USERNAME = os.environ['CLIENT_USERNAME']
    _CLIENT_PASSWORD= os.environ['CLIENT_PASSWORD']
    _LOG_FILE_PATH = os.environ['LOG_FILE_PATH']
    _LOGGING_LEVEL_STRING = os.environ['LOG_LEVEL']
    _TSDB_URL = os.environ["TSDB_URL"]
    _TSDB_PORT = os.environ["TSDB_PORT"]
    _TSDB_PROTOCOL = os.environ["TSDB_PROTOCOL"]
    _TSDB_TOKEN = os.environ["TSDB_TOKEN"]
    _TSDB_ORG = os.environ["TSDB_ORG"]
    _TSDB_BUCKET = os.environ["TSDB_BUCKET"]
    if _LOGGING_LEVEL_STRING == "DEBUG":
        _LOGGING_LEVEL = logging.DEBUG
    elif _LOGGING_LEVEL_STRING == "INFO":
        _LOGGING_LEVEL = logging.INFO
    elif _LOGGING_LEVEL_STRING == "WARNING":
        _LOGGING_LEVEL = logging.WARNING
    else:
        raise ValueError(f"Unexpected logging level provided: {_LOGGING_LEVEL_STRING}")

except KeyError:
    # No environmental variables provided, attempt to read from config.
    with open("/etc/mqtt-logger/mqtt_logger.yaml", "r") as f:
    #with open("local-test.yaml", "r") as f:
        config = yaml.safe_load(f)
    _BROKER_URL = config['MQTT_URL']
    _BROKER_PORT = config['MQTT_PORT']
    _CERT_PATH = config['CERT_PATH']
    _CLIENT_ID = config['CLIENT_ID']
    _CLIENT_USERNAME = config['CLIENT_USERNAME']
    _CLIENT_PASSWORD = config['CLIENT_PASSWORD']
    _LOG_FILE_PATH = config['LOG_FILE_PATH']
    _LOGGING_LEVEL_STRING = config['LOG_LEVEL']
    _TSDB_URL = config["TSDB_URL"]
    _TSDB_PORT = config["TSDB_PORT"]
    _TSDB_PROTOCOL = config["TSDB_PROTOCOL"]
    _TSDB_USERNAME = config["TSDB_USERNAME"]
    _TSDB_PASSWORD = config["TSDB_PASSWORD"]
    _TSDB_ORG = config["TSDB_ORG"]
    _TSDB_BUCKET = config["TSDB_BUCKET"]
    if _LOGGING_LEVEL_STRING == "DEBUG":
        _LOGGING_LEVEL = logging.DEBUG
    elif _LOGGING_LEVEL_STRING == "INFO":
        _LOGGING_LEVEL = logging.INFO
    elif _LOGGING_LEVEL_STRING == "WARNING":
        _LOGGING_LEVEL = logging.WARNING
    else:
        raise ValueError(f"Unexpected logging level provided: {_LOGGING_LEVEL_STRING}")

# We want all messages with no duplicates.
_QOS_LEVEL = 2
is_connected = False


def _fix_path(file_name: str) -> str:
    if _CERT_PATH.endswith("/"):
        return _CERT_PATH + file_name
    else:
        return _CERT_PATH + "/" + file_name


def _format_message(payload: bytes) -> Tuple[str, int]:
    # Could add more elegant processing here for other types of data.
    payload_dict = json.loads(payload)
    _logger.info(f'GOT: {payload_dict}')
    return payload_dict['timestamp'], payload_dict['data']


def _main():
    logging.basicConfig(level=_LOGGING_LEVEL)

    _logger.info('Running main')

    client = mqtt.Client()
    client.tls_set(_fix_path("ca.cert.pem"))
    client.tls_insecure_set(False)
    client.username_pw_set(_CLIENT_USERNAME, password=_CLIENT_PASSWORD)

    client._on_connect = _on_connect
    client._on_message = _on_message

    client.connect(_BROKER_URL, int(_BROKER_PORT))
    client.subscribe("#", qos=_QOS_LEVEL)
    client.loop_start()

    while not is_connected:
        _logger.debug("Connecting...")
        time.sleep(5)
    while True:
        # _on_message shall take care of logging.
        time.sleep(1)


def _on_connect(client, userdata, flags, rc):
    if rc == 0:
        global is_connected
        is_connected = True
        _logger.debug("Connected to broker")
    else:
        is_connected = False
        _logger.debug("Failed to connected with result " + str(rc))


def _on_disconnect(client, userdata, rc):
    if rc != 0:
        _logger.debug("Unexpected disconnection.")


def _on_message(client, userdata, message):
    _logger.debug(f"Received message [{message.topic}] - {message.payload}")
    _store_message(message.topic, message.payload)


def _store_message(topic, payload):
    timestamp, message = _format_message(payload)
    with open(_LOG_FILE_PATH, "a") as f:
        # Append the topic-message to the log file.
        f.write(f"{topic} {message}\n")

    topic_parts = topic.split("/")

    _logger.info(f"Received {topic} - {message}")

    url = f'{_TSDB_PROTOCOL}://{_TSDB_URL}:{_TSDB_PORT}'
    with InfluxDBClient(url=url,
                        username=_TSDB_USERNAME,
                        password=_TSDB_PASSWORD,
                        org=_TSDB_ORG) as client:
        p = Point("/".join(topic_parts[0:2])) \
        .field("/".join(topic_parts[2:]), message) \
        .time(datetime.datetime.fromtimestamp(timestamp), WritePrecision.MS)

        with client.write_api(write_options=SYNCHRONOUS) as write_api:
            write_api.write(bucket=_TSDB_BUCKET, record=p)


if __name__ == "__main__":
    _main()

