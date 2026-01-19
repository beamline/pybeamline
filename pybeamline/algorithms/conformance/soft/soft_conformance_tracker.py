from collections import deque
from typing import Dict

from pybeamline.algorithms.conformance.soft.soft_conformance_report import SoftConformanceReport
from pybeamline.algorithms.conformance.soft.soft_conformance_status import SoftConformanceStatus
from pybeamline.models.pdfa.pdfa import Pdfa


class SoftConformanceTracker:

    def __init__(self, model: Pdfa, max_cases_to_store: int = 1000):
        self._content: Dict[str, SoftConformanceStatus] = {}
        self._case_id_history: deque[str] = deque()
        self.model = model
        self.max_cases_to_store = max_cases_to_store

    def replay(self, case_id: str, new_event_name: str) -> SoftConformanceStatus:
        if case_id in self._content:
            cs = self._content[case_id]
            cs.replay_event(new_event_name)
            try:
                self._case_id_history.remove(case_id)
            except ValueError:
                pass
        else:
            if len(self._case_id_history) >= self.max_cases_to_store:
                oldest_case = self._case_id_history.popleft()
                self._content.pop(oldest_case, None)

            cs = SoftConformanceStatus(self.model, case_id)
            cs.replay_event(new_event_name)
            self._content[case_id] = cs

        self._case_id_history.append(case_id)

        return self._content[case_id]

    def get_report(self) -> SoftConformanceReport:
        report = SoftConformanceReport()
        report.put_all(self._content)
        return report

    def __getitem__(self, case_id: str) -> SoftConformanceStatus:
        return self._content[case_id]

    def __setitem__(self, case_id: str, value: SoftConformanceStatus):
        self._content[case_id] = value
        if case_id not in self._case_id_history:
            self._case_id_history.append(case_id)

    def __delitem__(self, case_id: str):
        self._content.pop(case_id, None)
        try:
            self._case_id_history.remove(case_id)
        except ValueError:
            pass

    def __contains__(self, case_id: str) -> bool:
        return case_id in self._content

    def __len__(self) -> int:
        return len(self._content)

    def items(self):
        return self._content.items()

    def keys(self):
        return self._content.keys()

    def values(self):
        return self._content.values()

    def is_empty(self) -> bool:
        return len(self._content) == 0