from pyais.stream import TCPConnection
from pybeamline.bevent import BEvent


def _fetch_ais_source(observer, host, port):
    for msg in TCPConnection(host, port=port):
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
            observer.on_next(e)

    observer.on_completed()