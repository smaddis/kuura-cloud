#!/usr/bin/python3
"""
Module for logging data from a car using OBD-II adapter.
"""
import argparse
import binascii
import json
import logging
import os
import time

import obd
import yaml

from obd.OBDResponse import Status

import paho.mqtt.client as mqtt


# TODO: One character logging level
_LOG_FORMAT = '%(asctime)s - %(module)s.%(funcName)s:%(lineno)d [%(levelname)s]: %(message)s'
_LOG_LEVEL = 'INFO'
_logger = logging.getLogger(__name__)
is_connected = False
_QOS_LEVEL = 2

_FILTER_OUT = [
    'O2_SENSORS',
    'O2_S1_WR_CURRENT',
    'AMBIANT_AIR_TEMP',
    'CALIBRATION_ID',
    'CATALYST_TEMP_B1S2',
    'CONTROL_MODULE_VOLTAGE',
    'CVN',
    'DISTANCE_W_MIL',
    'DISTANCE_SINCE_DTC_CLEAR',
    'DISTANCE_SINCE_DTC_CLEAR',
    'DTC_FUEL_TYPE',
    'DTC_BAROMETRIC_PRESSURE',
    'ELM_VOLTAGE',
    'FUEL_TYPE',
    'GET_CURRENT_DTC',
    'GET_DTC',
    'COMMANDED_EQUIV_RATIO',
    'EVAPORATIVE_PURGE',
    'LONG_FUEL_TRIM_1',
    'O2_S2_WR_CURRENT',
    'FUEL_STATUS',
    'BAROMETRIC_PRESSURE',
    'MIDS_A',
    'MIDS_B',
    'MIDS_C',
    'MIDS_D',
    'MIDS_E',
    'MIDS_F',
    'MONITOR_CATALYST_B1',
    'MONITOR_MISFIRE_CYLINDER_1',
    'MONITOR_MISFIRE_CYLINDER_2',
    'MONITOR_MISFIRE_CYLINDER_3',
    'MONITOR_MISFIRE_CYLINDER_4',
    'MONITOR_MISFIRE_GENERAL',
    'MONITOR_O2_B1S1',
    'MONITOR_O2_B1S2',
    'MONITOR_EGR_B1',
    'MONITOR_VVT_B1',
    'PIDS_9A',
    'PIDS_A',
    'PIDS_B',
    'PIDS_C',
    'RUN_TIME_MIL',
    'SHORT_O2_TRIM_B1',
    'LONG_O2_TRIM_B1',
    'SHORT_FUEL_TRIM_1',
    'STATUS',
    'TIMING_ADVANCE',
    'VIN',
    'WARMUPS_SINCE_DTC_CLEAR',
    'ELM_VERSION',
    'OBD_COMPLIANCE',

    'INTAKE_TEMP',
    'COOLANT_TEMP',
]


def _argument_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser()
    p.add_argument('logfile',
                   type=str,
                   help='Path to the logfile to be written.')
    p.add_argument('--force-overwrite', '-f',
                   dest='force_overwrite',
                   action='store_true',
                   default=False,
                   help='Overwrite the logfile if it already exists.')
    p.add_argument('--device',
                   type=str,
                   default='/dev/rfcomm99',
                   help='Path to the serial port-like device offering an interface to the car.')
    p.add_argument('--config',
                   type=str,
                   help='Path to a .yaml config file.')
    p.add_argument('--publish',
                   action='store_true',
                   default=False,
                   help='Publish the collected data into cloud.')
    p.add_argument('--debug',
                   action='store_true',
                   default=False,
                   help='Enable debug prints.')
    return p


def on_connect(client, userdata, flags, rc):
    if rc==0:
        global is_connected
        is_connected = True
        print("Connected to broker")
    else:
        is_connected = False
        print("Failed to connected with result " + str(rc))


def on_disconnect(client, userdata, rc):
    global is_connected
    if rc != 0:
        print("Unexpected disconnection.")
        is_connected = False


def on_publish(client, userdata, result):
    #_logger.debug("Msg id published: " + str(result))
    pass


def _fix_path(path: str, file_name: str) -> str:
    if path.endswith("/"):
        return path + file_name
    else:
        return path + "/" + file_name


def _log_car_data(connection,
                  logfile: str,
                  config_path: str = None,
                  publish: bool = False):
    commands = connection.supported_commands

    if publish:
        # We would like to publish the data also to the MQTT broker.
        global is_connected
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)
        mqtt_broker_url = config['MQTT_URL']
        mqtt_broker_port = config['MQTT_PORT']
        cert_path = config['CERT_PATH']
        client_id = config['CLIENT_ID']
        client_username = config['CLIENT_USERNAME']
        client_password = config['CLIENT_PASSWORD']

        client = mqtt.Client()
        client.tls_set(_fix_path(cert_path, 'ca.cert.pem'))
        client.tls_insecure_set(False)
        client.username_pw_set(client_username, password=client_password)
        client.on_connect = on_connect
        client.on_disconnect = on_disconnect
        client.on_publish = on_publish
        client.connect(mqtt_broker_url, int(mqtt_broker_port))
        client.loop_start()

        while not is_connected:
            print("Connecting...")
            time.sleep(5)

    # Keep the logfile open for the entire data reading.
    with open(logfile, 'w') as f:
        while True:
            try:
                # Loop through the supported commands and request them one by
                # one.
                for command in commands:

                    if command.name in _FILTER_OUT or 'DTC' in command.name or 'CATALYST' in command.name:
                        # FILTERED
                        #_logger.debug(f"SKIPPING {command.name}")
                        continue

                    response = connection.query(command)
                    ts = int(response.time)
                    val_decoded = response.value

                    if val_decoded is None:
                        _logger.debug(f'Value for {command.name} is none.')
                        continue
                    # TODO: Consider format.
                    val_raw = ';'.join([message.hex().decode() for message in response.messages])

                    if command == obd.commands.ELM_VOLTAGE :
                        val_raw = binascii.hexlify(str(val_decoded.magnitude).encode()).decode()
                    if command == obd.commands.ELM_VERSION:
                        val_raw = binascii.hexlify(val_decoded.encode()).decode()

                    if isinstance(val_decoded, Status):
                        val_decoded = f'{val_decoded.MIL};{val_decoded.DTC_count};{val_decoded.ignition_type}'
                    elif isinstance(val_decoded, obd.Unit.Quantity):
                        val_decoded = f'{val_decoded.magnitude}:{val_decoded.units}'
                    else:
                        val_decoded = str(val_decoded)

                    if isinstance(val_decoded, str):
                        val_decoded = val_decoded.replace('\n', ';')

                    log_line = f'{ts},{command.name},{val_decoded},{val_raw}\n'
                    #_logger.debug(log_line)
                    f.write(log_line)
                    f.flush()

                    if publish:
                        # Attempt to publish data via MQTT.
                        if not is_connected:
                            # Socket broke.
                            client.loop_stop()
                            client.disconnect()
                            raise RuntimeError(f'MQTT socket crashed.')

                        val_decoded = val_decoded[:val_decoded.find(':')]

                        # TODO: VAL mapping for command -> topic.
                        topic = f'{client_id}/toyota/{command.name}'
                        # TODO: Probably need to parse the data format.
                        payload = json.dumps({
                            'timestamp': time.time(),
                            'data': val_decoded,
                            'raw': val_raw,
                        })
                        _logger.debug(f'Publishing {topic} {payload}')
                        client.publish(topic=topic,
                                       payload=payload,
                                       qos=_QOS_LEVEL,
                                       retain=False)

            except KeyboardInterrupt:
                # Controlled way of stopping logging.
                break


def main():
    parsed_args = _argument_parser().parse_args()
    if parsed_args.debug:
        log_level = logging.DEBUG
        #obd.logger.setLevel(obd.logging.DEBUG)
    else:
        log_level = _LOG_LEVEL

    logging.basicConfig(level=log_level,
                        format=_LOG_FORMAT)

    _logger.debug('Debug prints enabled')

    connection = obd.OBD(parsed_args.device, fast=False)
    if connection.status() == obd.OBDStatus.NOT_CONNECTED:
        raise RuntimeError(f'Could not connect to car via {parsed_args.device}.')

    if os.path.exists(parsed_args.logfile):
        _logger.warning(f'Path "{parsed_args.logfile}" exists already.')
        if not parsed_args.force_overwrite:
            raise RuntimeError("Will not overwrite. Remove/move the logfile or"
                               " use force flag (\"-f\") to overwrite.")

    _log_car_data(connection,
                  parsed_args.logfile,
                  parsed_args.config,
                  parsed_args.publish)
    _logger.info('Successfully exited logger.')


if __name__ == '__main__':
    main()

