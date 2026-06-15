from dataclasses import dataclass, field
from typing import List, Tuple, Optional, Union

from pm4py.objects.petri_net.obj import PetriNet, Marking

@dataclass
class AlignmentState:

    alignment_step: Tuple[Optional[str], Optional[PetriNet.Transition]] = field(default_factory=lambda: (None, None))
    marking: Optional[Marking] = None
    cost: float = 0.0

    def __deepcopy__(self, memo):
        if id(self) in memo:
            return memo[id(self)]

        alignment = AlignmentState()
        memo[id(self)] = alignment

        alignment.alignment_step = self.alignment_step
        alignment.marking = Marking(self.marking) if self.marking is not None else None
        alignment.cost = self.cost

        return alignment


@dataclass
class CaseState:

    alignment: List[AlignmentState] = field(default_factory=list)

    def get_last(self) -> Optional[AlignmentState]:
        return self.alignment[-1] if len(self.alignment) > 0 else None

    def revert(self, n: int = None) -> 'CaseState':
        l = len(self.alignment)
        if n is None:
            n = l
        n = min(n, l)
        split_idx = l - n

        reverted_alignment = self.alignment[split_idx:]

        reverted_section = CaseState(alignment=reverted_alignment)
        self.alignment = self.alignment[:split_idx]
        return reverted_section

    def join(self, other: 'CaseState'):
        self.alignment += other.alignment

    def pretty_str(self):

        def calculate_padding(n, k):
            total_spaces = n - k
            if total_spaces < 0:
                return 0, 0

            before = total_spaces // 2
            after = total_spaces - before

            return before, after

        def alignment_element_to_str(step: Union[str, PetriNet.Transition]) -> str:
            if isinstance(step, str):
                return step
            return step.name

        ret = '\n'

        size = 0
        for alignment in self.alignment:
            size = max(
                size,
                2 if alignment.alignment_step[0] is None else len(alignment.alignment_step[0]),
                2 if alignment.alignment_step[1] is None else len(alignment.alignment_step[1].name)
            )
        size += 2
        for i in [0,2,1]:
            ret += '|'
            for alignment in self.alignment:
                if i == 2:
                    label = size * '-'
                else:
                    label = '>>' if alignment.alignment_step[i] is None else alignment_element_to_str(alignment.alignment_step[i])
                before_patting, after_padding = calculate_padding(size, len(label))
                ret += before_patting * ' ' + label + after_padding * ' '
                ret += '|'
            ret += '\n'
        return ret


