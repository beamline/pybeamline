from pybeamline.algorithms.conformance.Templates.Alternate import Alternate
from pybeamline.algorithms.conformance.Templates.Chain import Chain
from pybeamline.algorithms.conformance.Templates.Response import Response
class Constraint():

    def __init__(self, template) -> None:
        match template:
            case "Response":
                self.templ = Response()
            case "Alternate":
                self.templ = Alternate()
            case "Chain":
                self.templ = Chain()
            case _:
                raise Exception("Template not found")
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
    fulfillments = set()
    violations = set()
    template = constr.templ

    Template().opening()
    for e in trace:
        Template().fullfillment()
        Template().violation()
        Template().activation()

    Template().closing()

    return violations, fulfillments
    
# some form of templates
# some form of constraints -> probably own object type