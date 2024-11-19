from typing import Protocol

class Template(Protocol):

    def __init__(self) -> None:
        pass

    def opening(self):
        pass

    def closing(self, pending, fulfillments, violations):
        
        return pending, violations

    def fullfillment(self, e, trace, pending, fulfillments, T, phi_a, phi_c, phi_tau):
        
        return pending, fulfillments

    def violation(self, e, trace, pending, violations, T, phi_c, phi_tau):
        
        return pending, violations

    def activation(self, e, A, pending, phi_a):
        
        return pending

    def phi_activity(e):
        return "name?"

    def verify(phi_a, A):
        return True

    def verify(phi_c, A, B)
        return True

    def verify(phi_tau, A, B):
        return True