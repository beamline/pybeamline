import unittest
from datetime import timedelta, datetime
from typing import Callable

from build.lib.pybeamline.stream.stream import Stream
from pybeamline.algorithms.conformance.soft.pdfa_conformance import PdfaConformanceMapper
from pybeamline.algorithms.conformance.soft.soft_conformance_report import SoftConformanceReport
from pybeamline.bevent import BEvent
from pybeamline.models.pdfa.pdfa import Pdfa
from pybeamline.stream.base_sink import BaseSink


class TestSoftConformance(unittest.TestCase):



    def setUp(self):
        base_time = datetime(2025, 1, 1, 9, 0, 0)
        self.events = [
            BEvent("A", "case-0", event_time=base_time),
            BEvent("B", "case-0", event_time=base_time + timedelta(seconds=5)),
            BEvent("C", "case-0", event_time=base_time + timedelta(seconds=15)),
            BEvent("D", "case-0", event_time=base_time + timedelta(seconds=30)),
            BEvent("E", "case-0", event_time=base_time + timedelta(seconds=35)),

            BEvent("A", "case-1", event_time=base_time + timedelta(seconds=40)),
            BEvent("B", "case-1", event_time=base_time + timedelta(seconds=45)),
            BEvent("C", "case-1", event_time=base_time + timedelta(seconds=50)),
            BEvent("K", "case-1", event_time=base_time + timedelta(seconds=55)),
            BEvent("E", "case-1", event_time=base_time + timedelta(seconds=60))
        ]


    def construct_reference_model(self) -> Pdfa:
        pdfa = Pdfa()
        pdfa.add_node("A")
        pdfa.add_node("B")
        pdfa.add_node("C")
        pdfa.add_edge("A", "A", 0.2)
        pdfa.add_edge("A", "B", 0.8)
        pdfa.add_edge("B", "C", 1.0)
        return pdfa

    def test_real_case_scenario(self):

        class AssertSink(BaseSink[SoftConformanceReport]):

            def __init__(self, asser_equals: Callable):
                self.asser_equals = asser_equals
                self.counter = 0
                self.expected_soft_conf_values = [0.85,0.6167, 0.4625, 0.0, 0.4625, 0.925, 0.4625, 0.4625]
                self.expected_mean_probs = [0.5667, 0.4111, 0.3083, 0.0, 0.3083, 0.6167, 0.3083, 0.3083]

            def consume(self, item: SoftConformanceReport) -> None:
                for case_id in item.keys():
                    round_soft_value = round(item[case_id].get_soft_conformance(), 4)
                    self.asser_equals(self.expected_soft_conf_values[self.counter], round_soft_value)
                    round_mean_prob = round(item[case_id].get_mean_probabilities(), 4)
                    self.asser_equals(self.expected_mean_probs[self.counter], round_mean_prob)
                    self.counter += 1

        reference_model = self.construct_reference_model()
        mapper = PdfaConformanceMapper(reference_model, 0.5, 100, 2)

        Stream.from_iterable(self.events).pipe(mapper).sink(AssertSink(self.assertEqual))
