import json
import requests
import hashlib
from pybeamline.bevent import BEvent
from pybeamline.stream.base_source import BaseSource

class WikimediaSource(BaseSource[BEvent]):

    def execute(self):
        url = "https://stream.wikimedia.org/v2/stream/recentchange"
        headers = {"Accept": "application/json"}

        with requests.get(url, headers=headers, stream=True) as response:
            if response.status_code == 200:
                for line in response.iter_lines():
                    if line:
                        try:
                            data = json.loads(line.decode("utf-8"))
                            e = BEvent(data["type"], hashlib.md5(data["title_url"].encode()).hexdigest(), data["wiki"])
                            attributes_to_copy = ["title", "user", "bot", "comment", "server_url", "server_name",
                                                  "namespace", "revision", "length", "timestamp", "wiki"]
                            for k in attributes_to_copy:
                                if k in data:
                                    e.event_attributes[k] = data[k]
                            self.produce(e)
                        except json.JSONDecodeError as e:
                            self.error(e)
            else:
                print('error: ', response)
                self.error(Exception(f"Error: {response.status_code}"))

        self.completed()