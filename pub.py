# -*- coding: utf-8 -*-

import argparse
import time
import random
import json

import paho.mqtt.client as mqtt


def generate(type):
    if type == 'float':
        return 60 * random.random()
    elif type == 'int':
        return random.randint(0, 1000)
    elif type == 'str':
        return random.choice(
            ['normal', 'default', 'problem', 'issue', 'defect', 'fault'])
    else:
        return None


def get_message(structure):
    message = {}
    for key, value in structure.items():
        message[key] = generate(type=value)
    return json.dumps(message)


def generator(broker, port, topic, qos, delay, structure):
    mqttc = mqtt.Client()
    mqttc.connect(broker, port=port)
    mqttc.loop_start()
    while True:
        try:
            message = get_message(structure)
            mqttc.publish(topic, payload=message, qos=qos)
            time.sleep(delay)
        except KeyboardInterrupt:
            break


def main():
    parser = argparse.ArgumentParser(
        description='MQTT publisher message generator')
    parser.add_argument(
        '-b', '--broker',
        type=str,
        help='MQTT broker IP adress or hostname',
        required=True)
    parser.add_argument(
        '-p', '--port',
        type=int,
        help='MQTT broker port',
        default=1883)
    parser.add_argument(
        '-t', '--topic',
        type=str,
        help='MQTT topic',
        required=True)
    parser.add_argument(
        '-q', '--qos',
        type=int,
        help='MQTT QoS',
        default=0)
    parser.add_argument(
        '-d', '--delay',
        type=float,
        help='Delay, in seconds, between messages',
        default=1.0)
    parser.add_argument(
        '-s', '--structure',
        type=str,
        help='JSON representing the message structure: '
        'the key represents the name of the variable '
        'and the value represents its type. Example: '
        '\{temperature : float\}',
        required=True)
    args = parser.parse_args()
    generator(
        broker=args.broker,
        port=args.port,
        topic=args.topic,
        qos=args.qos,
        delay=args.delay,
        structure=json.loads(args.structure))


if __name__ == '__main__':
    main()
