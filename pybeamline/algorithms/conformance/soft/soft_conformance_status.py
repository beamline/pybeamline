import math
import time
from datetime import datetime
from typing import Optional

from pybeamline.models.incremental_mean import IncrementalMean
from pybeamline.models.pdfa.pdfa import Pdfa


class SoftConformanceStatus:

    def __init__(self, model: Pdfa, case_id: str):
        self.model: Pdfa = model
        self.case_id: str = case_id

        self.last_act: Optional[str] = None
        self.last_prob: float = 0.0
        self.prob: float = 1.0
        self.log_prob: float = 1.0

        self.mean = IncrementalMean()
        self.last_update: float = time.time()


    def replay_event(self, event_name: str) -> None:
        if self.last_act is not None:
            self.last_prob = self.model.get_sequence_probability(
                self.last_act, event_name
            )

            self.prob *= self.last_prob

            # Java: logProb += -Math.log(lastProb)
            if self.last_prob > 0.0:
                self.log_prob += -math.log(self.last_prob)
            else:
                self.log_prob = float("inf")

            self.mean.increment(self.last_prob)

        self.last_update = time.time()
        self.last_act = event_name

    def get_last_probability(self) -> float:
        return self.last_prob

    def get_sequence_probability(self) -> float:
        return self.prob

    def get_sequence_log_probability(self) -> float:
        return self.log_prob

    def get_mean_probabilities(self) -> float:
        return self.mean.get_result()

    def get_soft_conformance(self) -> float:
        mean_local = self.get_mean_probabilities()
        nodes = len(self.model.nodes)  # Pdfa stores nodes internally
        weight = self.model.weight_factor

        best = weight + ((1.0 / nodes) * (1.0 - weight))
        return mean_local / best if best > 0.0 else 0.0

    def get_last_update(self) -> datetime:
        return datetime.fromtimestamp(self.last_update)

    def get_case_id(self) -> str:
        return self.case_id

    def __eq__(self, other) -> bool:
        return (
            isinstance(other, SoftConformanceStatus)
            and self.case_id == other.case_id
        )

    def __hash__(self) -> int:
        return hash(self.case_id)

    def __str__(self) -> str:
        return (
            f"soft conformance: {self.get_soft_conformance()}, "
            f"mean of probabilities: {self.get_mean_probabilities()}"
        )
