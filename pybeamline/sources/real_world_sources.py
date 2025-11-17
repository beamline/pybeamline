from pybeamline.bevent import BEvent
from pybeamline.sources.real.ais import AisSource
from pybeamline.sources.real.rejseplanen import RejseplanenSource
from pybeamline.sources.real.wikimedia import WikimediaSource
from pybeamline.stream.stream import Stream


def wikimedia_source() -> Stream[BEvent]:
    return Stream.source(WikimediaSource())


def ais_source(host='153.44.253.27', port=5631) -> Stream[BEvent]:
    return Stream.source(AisSource(host, port))


def rejseplanen_source() -> Stream[BEvent]:
    return Stream.source(RejseplanenSource())






