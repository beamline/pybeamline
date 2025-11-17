from pyais.stream import TCPConnection
from pybeamline.bevent import BEvent
from pybeamline.stream.base_source import BaseSource

class AisSource(BaseSource[BEvent]):

    def __init__(self, host='153.44.253.27', port=5631):
        self.host = host
        self.port = port

    def execute(self):
        for msg in TCPConnection(self.host, port=self.port):
            decoded_message = msg.decode()
            ais_content = decoded_message
            if hasattr(ais_content, "status"):
                e = BEvent(ais_content.status.name, ais_content.mmsi, "AIS")
                e.event_attributes["mmsi"] = ais_content.mmsi
                e.event_attributes["speed"] = ais_content.speed
                e.event_attributes["accuracy"] = ais_content.accuracy
                e.event_attributes["lon"] = ais_content.lon
                e.event_attributes["lat"] = ais_content.lat
                e.event_attributes["course"] = ais_content.course
                e.event_attributes["heading"] = ais_content.heading
                e.event_attributes["second"] = ais_content.second
                self.produce(e)

        self.completed()