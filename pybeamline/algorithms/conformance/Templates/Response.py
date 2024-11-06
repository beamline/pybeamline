class Response():
    
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