from MP_Declare_Model import Constraint, MP_delcare_model
from pm4py.objects.log.importer.xes import importer
from Templates.Response import Response

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
    temp = Response() #Use correct template later

    #pending, fulfillments, violations = temp.opening()
    for e in trace:
        pending, fulfillments = temp.fullfillment(e, trace, pending, fulfillments, constraint.condition[0].T, constraint.condition[0].phi_a, constraint.condition[0].phi_c, constraint.condition[0].phi_tau)
        pending, violations = temp.violation(e, trace, pending, violations, constraint.condition[0].T, constraint.condition[0].phi_c, constraint.condition[0].phi_tau)
        pending = temp.activation(e, constraint.condition[0].A, pending, constraint.condition[0].phi_a)

    pending, violations = temp.closing(pending, fulfillments, violations)

    return violations, fulfillments

if __name__ == "__main__":
    model = MP_delcare_model.from_xml("pybeamline/algorithms/conformance/MultiperspectiveConformace/dummy_models/model-10-constraints-data.xml")
    log = open("pybeamline/algorithms/conformance/MultiperspectiveConformace/dommy_logs/10-acts-25000-traces.xes","r").read()
    viol = dict()
    fulfill = dict()
    
    viol, fulfill = check_log_conformance(log, model)

    print(fulfill)