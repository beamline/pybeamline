from typing import Protocol

class Template(Protocol):

    def __init__(self) -> None:
        pass

    def opening(self):
        pass

    def closing(self, pending, fulfillments, violations):
        pass

    def fullfillment(self, e, trace, pending, fulfillments, T, phi_a, phi_c, phi_tau):
        pass

    def violation(self, e, trace, pending, violations, T, phi_c, phi_tau):
        pass

    def activation(self, e, A, pending, phi_a):
        pass
