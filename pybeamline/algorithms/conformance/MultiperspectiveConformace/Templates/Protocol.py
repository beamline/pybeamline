from typing import Protocol

class Template(Protocol):

    def __init__(self) -> None:
        pass

    def opening(self):
        pass

    def closing(self, pending:set, fulfillments:set, violations:set):
        
        return pending, violations

    def fullfillment(self, e, trace, pending, fulfillments, T:set, phi_a, phi_c, phi_tau):
        
        return pending, fulfillments

    def violation(self, e, trace, pending:set, violations:set, T:set, phi_c, phi_tau):
        
        return pending, violations

    def activation(self, e, A:set, pending:set, phi_a):
        
        return pending

    def phi_activity(e):
        #example: phi_activity(e) in T: check that input event referes to a target
        return e.name

    def verify(phi_a, A:set):
        #A is a set of activations
        #phi_a is a activation condition
        #TODO: evaluate phi_a with respect to attributes reported in A
        #evaluate: Set of activations satisfies activation condition
        return True

    def verify(phi_c, A:set, B:set):
        #A and B are sets of attributes
        #phi_c is a correlation condition
        #TODO: evaluate phi_c with respect to the attributes defined in A and B
        #evaluate: there is a correlation between A and B that matches phi_c 
        return True

    def verify(phi_tau, A:set, B:set):
        #A and B are sets of attributes
        #phi_tau is a time condition
        #TODO: evaluate phi_c with respect to the attributes defined in A and B
        #evaluate: the time between A and B matches the condition states in phi_tau 
        return True