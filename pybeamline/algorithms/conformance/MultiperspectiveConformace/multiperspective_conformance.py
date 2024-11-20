from MP_Declare_Model import Constraint, MP_delcare_model
from pm4py.objects.log.importer.xes import importer as xes_importer

# Given a log and a model (a set of constraints), return the violations and fulfillments of the model on the log
def check_log_conformance(log, model: MP_delcare_model):
    viol = dict()
    fulfill = dict()

    for trace in log:

        if trace not in viol:
            viol[trace] = dict()

        if trace not in fulfill:
            fulfill[trace] = dict()

        for constraint in model.get_constraints():
            viol_res, fulfill_res = check_trace_conformance(trace, constraint)

            viol[trace][constraint] = viol_res

            fulfill[trace][constraint] = fulfill_res
    
    return viol, fulfill

# Given a trace and a constraint, return the violations and fulfillments of the constraint on the trace
def check_trace_conformance(trace, constraint:Constraint):
    pending = set()
    fulfillments = set()
    violations = set()

    pending, fulfillments, violations = constraint.template.opening() ##change to correcnt template
    for e in trace:
        pending, fulfillments = constraint.template.fullfillment(e, trace, pending, fulfillments, constraint.condition.T, constraint.phi_a, constraint.phi_c, constraint.phi_tau)
        pending, violations = constraint.template.violation(e, trace, pending, violations, constraint.T, constraint.phi_c, constraint.phi_tau)
        pending = constraint.template.activation(e, constraint.A, pending, constraint.phi_a)

    pending, violations = constraint.template.closing(pending, fulfillments, violations)

    return violations, fulfillments

if __name__ == "__main__":
    model = MP_delcare_model.from_xml("pybeamline/algorithms/conformance/MultiperspectiveConformace/dummy_models/model-10-constraints-data.xml")
    log = xes_importer.apply("10-acts-25000-traces.xes")

    for trace in log:
        for e in trace:
            print(e)
            break

    for constraint in model.constraints:
        print(constraint.condition)
        break

        