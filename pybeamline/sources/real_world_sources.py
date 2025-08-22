import reactivex
from pybeamline.sources.real.ais import _fetch_ais_source
from pybeamline.sources.real.wikimedia import _fetch_wikimedia_stream
from pybeamline.sources.real.rejseplanen import _fetch_rejseplanen_trains


def wikimedia_source():
    return reactivex.create(_fetch_wikimedia_stream)


def ais_source(host='153.44.253.27', port=5631):
    def fetch_with_params(observer, _):
        return _fetch_ais_source(observer, host, port)
    return reactivex.create(fetch_with_params)


def rejseplanen_source():
    return reactivex.create(_fetch_rejseplanen_trains)






