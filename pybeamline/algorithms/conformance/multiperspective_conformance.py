from pybeamline.algorithms.conformance.Templates.Alternate import Alternate
from pybeamline.algorithms.conformance.Templates.Chain import Chain
from pybeamline.algorithms.conformance.Templates.Response import Response
from pybeamline.algorithms.conformance.Templates.Protocol import Template
class Constraint():

    def __init__(self, template: Template) -> None:
        self.templ = template
        self.A = set()
        self.T = set()   
        self.phi_a = set()
        self.phi_c = set()
        self.phi_tau = set()

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

    template.opening()
    for e in trace:
        template.fullfillment()
        template.violation()
        template.activation()

    template.closing()

    return violations, fulfillments
    
# some form of templates
# some form of constraints -> probably own object type