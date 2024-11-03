class Template():

    def __init__(self) -> None:
        pass

    def opening(self):
        pass

    def closing(self):
        pass

    def fullfillment(self):
        pass

    def violation(self):
        pass

    def activation(self):
        pass

class Constraint():

    def __init__(self) -> None:
        self.templ = Template()
        self.A
        self.T
        self.phi_a
        self.phi_c
        self.phi_tau

#Let fulfill and viol be maps that, given a trace and a constraint, return the set of fulfilling and violating events
def check_log_conformance(log, model):
    viol = dict()
    fulfill = dict()

    for trace in log:
        for constr in model:
            viol_res, fulfill_res = check_trace_conformance(trace, constr)

            viol[trace][constr] = viol_res
            fulfill[trace][constr] = fulfill_res
    
    return viol, fulfill

def check_trace_conformance(trace, constr:Constraint):
    pending = set()
    fulfullments = set()
    violations = set()

    Template().opening()
    for e in trace:
        Template().fullfillment()
        Template().violation()
        Template().activation()

    Template().closing()

    return violations, fulfullments





    
# some form of templates
# some form of constraints -> probably own object type