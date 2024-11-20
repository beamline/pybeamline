import Protocol as temp

class Response():

    def phi_activity(e):
        return e.name
 
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
        
        if (temp.phi_activity(e) in T):
            for act in pending:
                if (temp.verify(phi_c,act,e) and temp.verify(phi_tau, act,e)):
                    pending.remove(act)
                    fulfillments.add(act)

        return pending, fulfillments

    def violation(self, e, trace, pending, violations, T, phi_c, phi_tau):
        
        return pending, violations

    def activation(self, e, A, pending, phi_a):

        if (temp.phi_activity(e) in A and temp.verify(phi_a,A)):
            pending.add(e)
        
        return pending   