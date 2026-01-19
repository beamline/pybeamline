import json

import paho.mqtt.client as mqtt

from pybeamline.bevent import BEvent
from pybeamline.stream.base_source import BaseSource
from pybeamline.stream.stream import Stream


def mqttxes_source(broker: str, port: int, base_topic: str) -> Stream[BEvent]:
    return Stream.source(MqttSource(broker, port, base_topic))


class MqttSource(BaseSource[BEvent]):

    def __init__(self, broker: str, port: int, base_topic: str) -> None:
        self.broker = broker
        self.port = port
        self.base_topic = base_topic

    def execute(self):
        def on_connect(client, userdata, flags, rc):
            if rc == 0:
                print("Connected to MQTT broker")
                if self.base_topic.endswith("/"):
                    client.subscribe(self.base_topic + '#')
                else:
                    client.subscribe(self.base_topic + '/#')
            else:
                self.error(Exception("Connection failed with code {rc}"))

        def on_message(client, userdata, msg):
            path = msg.topic.replace(self.base_topic, '').split('/')
            if path[0] == '':
                path.pop(0)
            activity_name = path[len(path) - 1]
            case = path[len(path) - 2]
            process = "Process"
            if len(path) > 2:
                process = path[len(path) - 3]
            e = BEvent(activity_name, case, process)
            try:
                if msg.payload.decode().strip() != '':
                    attributes = json.loads(msg.payload.decode())
                    for k in attributes:
                        e.event_attributes[k] = attributes[k]
            except json.JSONDecodeError:
                print("Error decoding JSON")
            self.produce(e)

        def on_disconnect(client, userdata, rc):
            if rc != 0:
                self.error(Exception(f"Unexpected disconnection. Code: {rc}"))
            else:
                self.completed()

        client = mqtt.Client()
        client.on_connect = on_connect
        client.on_message = on_message
        client.on_disconnect = on_disconnect

        client.connect(self.broker, self.port, 60)
        client.loop_start()

        def dispose():
            client.loop_stop()
            client.disconnect()

        return dispose