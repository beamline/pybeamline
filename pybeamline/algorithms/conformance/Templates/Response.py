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
        
        if (phi_activity(e) in T):
            for act in pending:
                if (phi_c(act, e) and phi_tau(act,e)):
                    pending.remove(act)
                    fulfillments.add(act)

        return pending, fulfillments

    def violation(self, e, trace, pending, violations, T, phi_c, phi_tau):
        
        return pending, violations

    def activation(self, e, A, pending, phi_a):

        if (phi_activity(e) in A and phi_a(e))
            pending.add(e)
        
        return pending   