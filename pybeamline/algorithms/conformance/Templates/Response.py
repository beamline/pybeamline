class Response():
    
    def __init__(self) -> None:
        pass

    def opening(self):
        pass

    def closing(self, pending, fulfillments, violations):

        for act in pending:
            pending.remove(act)
            violations.add(act)
        
        return pending, violations

    def fullfillment(self, e, trace, pending, fulfillments, T, phi_a, phi_c, phi_tau):
        
        return pending, fulfillments

    def violation(self, e, trace, pending, violations, T, phi_c, phi_tau):
        
        return pending, violations

    def activation(self, e, A, pending, phi_a):

        ##Pi_activity(e) select active name associated to e
        ##verify(phi_a,A) evaluates Phi_a with respect to the attributes reported in A
        ##Phi_a is the activation condition
        ##A is a set of all possible activities 
        
        return pending   