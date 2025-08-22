import json
import time
from datetime import datetime, timezone
from typing import Dict, Iterable

import reactivex as rx
from reactivex import operators as ops
import requests

from pybeamline.bevent import BEvent

URL = "https://webapp.rejseplanen.dk/bin/query.exe/mny"
BASE_PARAMS = {
	"look_minx": "7558593.750000001",
	"look_maxx": "13617553.710937502",
	"look_miny": "54278054.85967284",
	"look_maxy": "57453816.135622375",
	"tpl": "trains2json3",
	"look_json": "yes",
	"performLocating": "1",
	"look_requesttime": "",
	"look_nv": "get_ageofreport|yes|get_rtmsgstatus|no|get_rtfreitextmn|no|get_passproc|no|"
			   "interval|30000|intervalstep|5000|get_nstop|no|get_pstop|yes|"
			   "get_stopevaids|yes|tplmode|trains2json3|cats|014,048,003,004,005,017,030,031,065,099",
	"interval": "30000",
	"intervalstep": "5000",
	"ts": "",
}
HEADERS = {"User-Agent": "denmark-trains/1.0 (+https://example.org)"}
POLL_SECONDS = 5


def _fetch_trains(timeout: int = 15) -> Iterable[dict]:
	# print("Fetching trains from Rejseplanen...")
	params = BASE_PARAMS.copy()
	now_local = datetime.now(timezone.utc).astimezone()
	params["look_requesttime"] = now_local.strftime("%H:%M:%S")
	params["ts"] = now_local.strftime("%Y%m%d")

	r = requests.get(URL, params=params, headers=HEADERS, timeout=timeout)
	r.raise_for_status()
	payload = json.loads(r.text)

	for item in payload[0]:
		if len(item) > 8:
			yield {
				"id": item[3],
				"name": item[0],
				"last_station": item[9],
				"delay": item[6],
				"destination": item[7],
				"fetched_at": now_local,
			}


def _try_parse_int(s: str):
	try:
		return int(s)
	except ValueError:
		return 0


def _to_bevent(train: dict, prev_station: str) -> BEvent:
	ev = BEvent(train["last_station"], train["id"], "Rejseplanen")
	ev.trace_attributes["train-name"] = train["name"]
	ev.trace_attributes["destination"] = train["destination"]
	ev.event_attributes["train-delay"] = _try_parse_int(train["delay"])
	ev.event_attributes["train-previous-stop"] = prev_station
	return ev


def _fetch_rejseplanen_trains(observer, _):
	last_seen = {}
	for t in _fetch_trains():
		last_seen[t["id"]] = t["last_station"]

	while True:
		trains = _fetch_trains()
		for t in trains:
			key = t["id"]
			if key in last_seen:
				if last_seen[key] != t["last_station"]:
					# print(f"Train {t['name']} changed from {last_seen[key]} to {t['last_station']}")
					observer.on_next(_to_bevent(t, last_seen[key]))
				last_seen[key] = t["last_station"]
			else:
				# print(f"New train detected: {t['name']} at {t['last_station']}")
				last_seen[key] = t["last_station"]
		time.sleep(POLL_SECONDS)

	observer.on_completed()
