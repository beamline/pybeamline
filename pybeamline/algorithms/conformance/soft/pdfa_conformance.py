from typing import Optional, List

from pybeamline.algorithms.conformance.soft.soft_conformance_report import SoftConformanceReport
from pybeamline.algorithms.conformance.soft.soft_conformance_tracker import SoftConformanceTracker
from pybeamline.algorithms.conformance.soft.weights_normalizer import WeightsNormalizer
from pybeamline.bevent import BEvent
from pybeamline.models.pdfa.pdfa import Pdfa
from pybeamline.stream.base_map import BaseMap


class PdfaConformanceMapper(BaseMap[BEvent, SoftConformanceReport]):

    def __init__(self, model: Pdfa, alpha: float, max_cases_to_store: int = 1000, result_refresh_rate = 10):
        self.pdfa_conformance = PdfaConformance(model, alpha, max_cases_to_store)
        self.pdfa_conformance.set_results_refresh_rate(result_refresh_rate)

    def transform(self, value: BEvent) -> Optional[List[SoftConformanceReport]]:
        result = self.pdfa_conformance.ingest(value)
        if result is not None:
            return [result]
        return None

class PdfaConformance:

    def __init__(self, model: Pdfa, alpha: float, max_cases_to_store: int = 1000):
        if alpha is not None:
            model = WeightsNormalizer.normalize(model, alpha)
        self.tracker = SoftConformanceTracker(model, max_cases_to_store)
        self.attribute_for_discovery: Optional[str] = None
        self.results_refresh_rate: int = 10
        self._processed_events: int = 0


    def set_results_refresh_rate(self, results_refresh_rate: int) -> 'PdfaConformance':
        self.results_refresh_rate = results_refresh_rate
        return self

    def set_attribute_for_discovery(self, attribute_for_discovery: str) -> 'PdfaConformance':
        self.attribute_for_discovery = attribute_for_discovery
        return self


    def ingest(self, event: BEvent) -> Optional[SoftConformanceReport]:

        case_id = event.get_trace_name()
        if self.attribute_for_discovery is None:
            activity_name = event.get_event_name()
        else:
            activity_name = str(event.event_attributes().get(self.attribute_for_discovery))

        # Replay event
        self.tracker.replay(case_id, activity_name)

        # Increment processed events
        self._processed_events += 1

        if self._processed_events % self.results_refresh_rate == 0:
            return self.tracker.get_report()

        return None

    def get_processed_events(self) -> int:
        return self._processed_events
