"""Set-based Dynamic Condition Response (DCR) graph models."""

from dataclasses import dataclass
from typing import FrozenSet, Optional, Tuple


DCRRelation = Tuple[str, str]


@dataclass(frozen=True)
class DCRMarking:
    """A DCR graph marking: executed, pending responses, and included events."""

    executed: FrozenSet[str] = frozenset()
    pending: FrozenSet[str] = frozenset()
    included: FrozenSet[str] = frozenset()

    def __post_init__(self) -> None:
        object.__setattr__(self, "executed", frozenset(self.executed))
        object.__setattr__(self, "pending", frozenset(self.pending))
        object.__setattr__(self, "included", frozenset(self.included))


@dataclass(frozen=True)
class DCRGraph:
    """A DCR graph with a set of activities and four relation types.

    Relations are represented as ``(source_activity, target_activity)`` pairs.
    Unless an initial marking is supplied, every activity starts included,
    with no activities executed and no pending responses.
    """

    activities: FrozenSet[str]
    conditions: FrozenSet[DCRRelation] = frozenset()
    responses: FrozenSet[DCRRelation] = frozenset()
    includes: FrozenSet[DCRRelation] = frozenset()
    excludes: FrozenSet[DCRRelation] = frozenset()
    marking: Optional[DCRMarking] = None

    def __post_init__(self) -> None:
        activities = frozenset(self.activities)
        object.__setattr__(self, "activities", activities)
        for relation_name in ("conditions", "responses", "includes", "excludes"):
            object.__setattr__(self, relation_name, frozenset(getattr(self, relation_name)))
        if self.marking is None:
            object.__setattr__(
                self,
                "marking",
                DCRMarking(included=activities),
            )

    @property
    def initial_marking(self) -> DCRMarking:
        """Alias emphasizing that discovered graphs use their initial marking."""
        return self.marking

    @property
    def relations(self) -> FrozenSet[Tuple[str, str, str]]:
        """All graph relations as ``(source, target, relation_name)`` triples."""
        return frozenset(
            (source, target, relation_name)
            for relation_name, relation_set in (
                ("condition", self.conditions),
                ("response", self.responses),
                ("include", self.includes),
                ("exclude", self.excludes),
            )
            for source, target in relation_set
        )
