from pybeamline.algorithms.conformance.MultiperspectiveConformace.Templates import Template

class Model():
    def __init__(self) -> None:
        self.constraints = set()
    
    def add_constraint(self, constraint):
        self.constraints.add(constraint)

    def remove_constraint(self, constraint):
        self.constraints.remove(constraint)

    def get_constraints(self):
        return self.constraints
    
class Constraint():
    def __init__(constr) -> None:
        self.template = Templates.Response
        self.A = constr.A
        self.T = constr.add
        self.phi_a = phi_a
        self.phi_c = phi_c
        self.phi_tau = phi_tau

# Given a log and a model (a set of constraints), return the violations and fulfillments of the model on the log
def check_log_conformance(log, model: Model):
    viol = dict()
    fulfill = dict()

    for trace in log:

        if trace not in viol:
            viol[trace] = dict()

        if trace not in fulfill:
            fulfill[trace] = dict()

        for constr in model.get_constraints():
            constraint = Constraint(constr)
            viol_res, fulfill_res = check_trace_conformance(trace, constr)

            viol[trace][constr] = viol_res

            fulfill[trace][constr] = fulfill_res
    
    return viol, fulfill

# Given a trace and a constraint, return the violations and fulfillments of the constraint on the trace
def check_trace_conformance(trace, constraint:Constraint):
    pending = set()
    fulfillments = set()
    violations = set()

    pending, fulfillments, violations = constraint.template.opening()
    for e in trace:
        pending, fulfillments = constraint.template.fullfillment(e, trace, pending, fulfillments, constraint.T, constraint.phi_a, constraint.phi_c, constraint.phi_tau)
        pending, violations = constraint.template.violation(e, trace, pending, violations, constraint.T, constraint.phi_c, constraint.phi_tau)
        pending = constraint.template.activation(e, constraint.A, pending, constraint.phi_a)

    pending, violations = constraint.template.closing(pending, fulfillments, violations)

    return violations, fulfillments