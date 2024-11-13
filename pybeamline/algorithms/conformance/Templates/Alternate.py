def phi_activity(e):
    return e.name

class Alternate():
    
    def __init__(self, constraint) -> None:
        self.possibleTargets = set()
        self.constraint = constraint

    def opening(self):
        self.possibleTargets = set()

    def closing(self, pending, fulfillments : set, violations : set, phi_c, phi_tau): # added phi_c, phi_tau
        if (len(pending) == 1):
            targetFound = False
            act = pending[0]
            for p in self.possibleTargets:
                if (phi_c(act, p) and (phi_tau(act, p))):
                    targetFound = True
                    fulfillments = fulfillments.add(act)
            if (not targetFound):
                violations = violations.add(act)
        return pending, violations

    def fullfillment(self, e, trace, pending : set, fulfillments : set, T, phi_a, phi_c, phi_tau):
        for phi_activity(e) in self.constraint.A:
            if (phi_a(e)):
                if (len(self.possibleTargets) >= 1 and len(pending) >= 1):
                    act = pending[0] # only 1 element
                    for p in self.possibleTargets:
                        if (phi_c(act, p) and phi_tau(act, p)):
                            fulfillments = fulfillments.add(act)
                            pending = pending.remove(act)
        for e in T:
            self.possibleTargets = self.possibleTargets.add(e)

        return pending, fulfillments

    def violation(self, e, trace, pending : set, violations : set, T, phi_c, phi_tau, phi_a): # added phi_a
        for phi_activity(e) in self.constraint.A:
            if (phi_a(e) and len(pending) == 1):
                act = pending[0]
                pending = pending.remove(act)
                violations = violations.add(act)
        return pending, violations

    def activation(self, e, A, pending, phi_a):
        for phi_activity(e) in self.constraint.A:
            if (phi_a(e)):
                self.possibleTargets = set()
                pending = pending.add(e)

        return pending  