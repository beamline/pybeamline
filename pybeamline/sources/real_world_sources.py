import json
import reactivex
import requests
import hashlib
from pyais.stream import TCPConnection
from pybeamline.bevent import BEvent
from functools import partial


def wikimedia_source():
    return reactivex.create(_fetch_wikimedia_stream)


def ais_source(host='153.44.253.27', port=5631):
    def fetch_with_params(observer, _):
        return _fetch_ais_source(observer, host, port)
    return reactivex.create(fetch_with_params)


def _fetch_wikimedia_stream(observer, _):
    url = "https://stream.wikimedia.org/v2/stream/recentchange"
    headers = {"Accept": "application/json"}

    with requests.get(url, headers=headers, stream=True) as response:
        if response.status_code == 200:
            for line in response.iter_lines():
                if line:
                    try:
                        data = json.loads(line.decode("utf-8"))
                        e = BEvent(data["type"], hashlib.md5(data["title_url"].encode()).hexdigest(), data["wiki"])
                        attributes_to_copy = ["title", "user", "bot", "comment", "server_url", "server_name", "namespace", "revision", "length", "timestamp", "wiki"]
                        for k in attributes_to_copy:
                            if k in data:
                                e.event_attributes[k] = data[k]
                        observer.on_next(e)
                    except json.JSONDecodeError as e:
                        observer.on_error(e)
        else:
            observer.on_error(Exception(f"Error: {response.status_code}"))

    observer.on_completed()


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
