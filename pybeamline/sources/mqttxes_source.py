import json
from typing import Optional

import paho.mqtt.client as mqtt
from reactivex import Observable, abc

from pybeamline.bevent import BEvent


def mqttxes_source(
        broker: str,
        port: int,
        base_topic: str) -> Observable[BEvent]:

    def subscribe(
            observer: abc.ObserverBase[BEvent],
            scheduler_: Optional[abc.SchedulerBase] = None
    ) -> abc.DisposableBase:
        def on_connect(client, userdata, flags, rc):
            if rc == 0:
                print("Connected to MQTT broker")
                if base_topic.endswith("/"):
                    client.subscribe(base_topic + '#')
                else:
                    client.subscribe(base_topic + '/#')
            else:
                observer.on_error(f"Connection failed with code {rc}")

        def on_message(client, userdata, msg):
            path = msg.topic.replace(base_topic, '').split('/')
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
            observer.on_next(e)

        def on_disconnect(client, userdata, rc):
            if rc != 0:
                observer.on_error(f"Unexpected disconnection. Code: {rc}")
            else:
                observer.on_completed()

        client = mqtt.Client()
        client.on_connect = on_connect
        client.on_message = on_message
        client.on_disconnect = on_disconnect

        client.connect(broker, port, 60)
        client.loop_start()

        def dispose():
            client.loop_stop()
            client.disconnect()

        return dispose

    return Observable(subscribe)
