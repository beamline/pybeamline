class Response():

    def phi_activity(self, e):
        #example: phi_activity(e) in T: check that input event referes to a target
        return "a1" #e.name

    def verify1(self, phi_a, A:set):
        #A is a set of activations
        #phi_a is a activation condition
        #TODO: evaluate phi_a with respect to attributes reported in A
        #evaluate: Set of activations satisfies activation condition
        return True

    def verify2(self, phi_c, A:set, B:set):
        #A and B are sets of attributes
        #phi_c is a correlation condition
        #TODO: evaluate phi_c with respect to the attributes defined in A and B
        #evaluate: there is a correlation between A and B that matches phi_c 
        return True

    def verify3(self, phi_tau, A:set, B:set):
        #A and B are sets of attributes
        #phi_tau is a time condition
        #TODO: evaluate phi_c with respect to the attributes defined in A and B
        #evaluate: the time between A and B matches the condition states in phi_tau 
        return True
 
    def __init__(self) -> None:
        pass

    def opening(self):
        pass

    def closing(self, pending, fulfillments, violations):

        for act in pending.copy():
            pending.remove(act)
            violations.add(act)
        
        return pending, violations

    def fullfillment(self, e, trace, pending, fulfillments, T, phi_a, phi_c, phi_tau):
        
        if (self.phi_activity(e) in T):
            for act in pending.copy():
                if (self.verify2(phi_c,act,e) and self.verify3(phi_tau, act,e)):
                    pending.remove(act)
                    fulfillments.add(act)

        return pending, fulfillments

    def violation(self, e, trace, pending, violations, T, phi_c, phi_tau):
        
        return pending, violations

    def activation(self, e, A, pending, phi_a):

        if (self.phi_activity(e) in A and self.verify1(phi_a,A)):
            pending.add(e)
        
        return pending