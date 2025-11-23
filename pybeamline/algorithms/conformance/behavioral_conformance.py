import sys
from collections import defaultdict
from typing import Optional, List

from pybeamline.bevent import BEvent
from pybeamline.stream.base_map import BaseMap, T, R


class BehavioralConformanceChecker(BaseMap[BEvent, tuple]):

    def __init__(self, model):
        self.model = model
        self.observed_events = 0
        self.bc = BehavioralConformance(M=model)

    def transform(self, event: BEvent) -> Optional[List[tuple]]:
        self.observed_events += 1
        self.bc.ingest_event(event)
        case_id = event.get_trace_name()
        if case_id in self.bc.get_conformance():
            return [(self.bc.get_conformance(case_id),
                     self.bc.get_confidence(case_id),
                     self.bc.get_completeness(case_id),
                     self.observed_events)]
        return None


def behavioral_conformance(model) -> BehavioralConformanceChecker:
    return BehavioralConformanceChecker(model)


def mine_behavioral_model_from_stream(source) -> tuple:
    bmb = BehavioralModelBuilder()
    source.subscribe(
        on_next=lambda i:
        {
            bmb.ingest_event(i),
        },
        on_error=lambda e, l: print("Error Occurred: {0} to {1}".format(e, l)),
    )
    bmb.end_xes_to_model()

    return bmb.get_model()


# Class originally developed by Magnus Frederiksen as part of his BSc project at DTU entitled
# "Development of Process Mining and Complex Event Processing using Python"
class BehavioralConformance:
    def __init__(self, M=([], defaultdict(list), defaultdict(list))):
        # the input is expecting to be the model
        self.set_model(M)  # calls method to set, B, P, and F
        self.__trace_last_event = dict()  # recalls last event for trace to find relation
        self.__conformance = dict()  # saves a traces' conformance
        self.__completeness = dict()  # saves a traces' completeness
        self.__confidence = dict()  # saves a traces' confidence
        self.__obs = defaultdict(list)  # saves all distinct relations in a trace that has ocurred
        self.__inc = dict()  # saves amount of incorrect relations acording to the reference model of a trace

    def ingest_event(self, event):
        case_id = event.get_trace_name()  # easier reference to caseID
        event_name = event.get_event_name()  # easier trefermce to eventNa,e

        if case_id not in self.__trace_last_event:  # if this is first time caseID appears
            self.__trace_last_event[case_id] = event_name  # save current event

            # self.__obs[caseID] = []
            self.__inc[case_id] = 0  # set amount of incorrect relations for that CaseId to 0

        else:  # if the caseID has been seen before

            new_pattern = (self.__trace_last_event[case_id], event_name)  # locally save the relation
            # Step 1: update internal data structures
            if new_pattern in self.__B:  # if the relation is in the approved relation list
                if new_pattern not in self.__obs[case_id]:  # and that relation has not occurred for that caseID before
                    self.__obs[case_id].append(new_pattern)  # save the relation to that CaseId
            else:  # if the relation is "illegal" according to B
                self.__inc[case_id] += 1  # increment incorrect for that caseID

            # Step 2: compute online conformance values
            self.__conformance[case_id] = len(self.__obs[case_id]) / (
                    len(self.__obs[case_id]) + self.__inc[case_id])  # calculated conformance

            if new_pattern in self.__B:  # if the relation is legal in B
                # if the relation occurrence is within P_min and P_max
                if self.__P[new_pattern][0] <= len(self.__obs[case_id]) and \
                        len(self.__obs[case_id]) <= self.__P[new_pattern][1]:
                    self.__completeness[case_id] = 1  # set completeness for that caseID
                else:  # if not within P_min and P_max
                    self.__completeness[case_id] = min(1, len(self.__obs[case_id]) / (
                            self.__P[new_pattern][0] + 1))  # calculate completeness

                self.__confidence[case_id] = 1 - (self.__F[new_pattern] / self.__maxOfMinRelationsAfter)
            else:
                self.__confidence[case_id] = -1
                self.__completeness[case_id] = -1

            # Step 3: cleanup
            self.__trace_last_event[case_id] = event_name

    def get_conformance(self, case_id=None):
        if case_id is None:
            return self.__conformance
        return self.__conformance[case_id]

    def get_completeness(self, case_id=None):
        if case_id is None:
            return self.__completeness
        return self.__completeness[case_id]

    def get_confidence(self, case_id=None):
        if case_id is None:
            return self.__confidence
        return self.__confidence[case_id]

    def get_model(self):
        return (self.__B, self.__P, self.__F)

    def set_model(self, M):
        self.__B = M[0]
        self.__P = M[1]
        self.__F = M[2]
        self.__maxOfMinRelationsAfter = 0
        for relations in self.__F:
            self.__maxOfMinRelationsAfter = max(self.__F[relations], self.__maxOfMinRelationsAfter)

    # def getMaxOfF(self):
    #     return self.__maxOfMinRelationsAfter
    #
    # def setM_FromXES(self, fileName):
    #     self.setM(model_from_XES().set_model_from_XES(fileName))

    # def printRefModel(self):
    #     G = pgv.Graph(comment='ref_model')  # setting type of graph
    #     G.graph_attr['rankdir'] = 'LR'
    #     G.node_attr['shape'] = 'box'
    #     G.edge_attr['arrowType'] = 'normal'
    #
    #     for (A, B) in self.__B:
    #         G.edge(A, B, label=str(), dir='forward')
    #
    #     G.render('model-output/ref_Model.gv').replace('\\', '/')


# Class originally developed by Magnus Frederiksen as part of his BSc project at DTU entitled
# "Development of Process Mining and Complex Event Processing using Python"
class BehavioralModelBuilder():
    def __init__(self):
        self.__B = []
        self.__P = dict()
        self.__F = dict()
        self.__tracelogs = defaultdict(list)
        self.__mark = []  # used to find P_max for all relation
        self.__mark2 = []

    def get_model(self):
        return self.__B, self.__P, self.__F

    def ingest_event(self, newEvent):
        case_id = newEvent.get_trace_name()  # locally save caseID
        event_name = newEvent.get_event_name()  # locally save event
        if case_id in self.__tracelogs:  # if this caseID has occured before
            relation = (self.__tracelogs[case_id][-1], event_name)  # find relation based on former event
            traceLength = len(self.__tracelogs[case_id]) - 1  # find the length of the trace so far
            if relation not in self.__B:  # if this relation already exist
                self.__B.append(relation)  # add relation

        else:  # if the trace hasent been seen yet
            self.__tracelogs[case_id] = []

        self.__tracelogs[case_id].append(event_name)  # add the event to the trace log

    def __setF(self, relation):
        Queue = []  # breath first search from an accepting state
        BFSMark = []
        BFSMark.append(relation)
        Queue.append((relation, 0))
        while len(Queue) > 0:
            ((A, B), depth) = Queue.pop(0)
            self.__F[(A, B)] = min(self.__F[(A, B)], depth)
            for (C, D) in self.__B:
                if D == A and (C, D) not in BFSMark:
                    BFSMark.append((C, D))
                    Queue.append(((C, D), depth + 1))

    def __findP_max(self, relation, depth):
        if relation not in self.__mark:  # brute force to find longest path
            self.__mark.append(relation)

            (P_min, P_max) = self.__P[relation]
            self.__P[relation] = (P_min, max(depth, P_max))

            (A, B) = relation
            for (C, D) in self.__B:
                if C == B and depth <= 5:
                    self.__findP_max((C, D), depth + 1)

            self.__mark.remove(relation)
        else:
            if relation in self.__mark2:
                return
            self.__mark2.append(relation)

            (P_min, P_max) = self.__P[relation]
            self.__P[relation] = (P_min, max(depth, P_max))
            (A, B) = relation
            for (C, D) in self.__B:
                if C == B and depth <= 5:
                    self.__findP_max((C, D), depth)

            self.__mark2.remove(relation)

    def __setP(self, relation):
        Queue = []  # breath first state from beggining state
        BFSMark = []
        BFSMark.append(relation)
        Queue.append((relation, 0))
        while len(Queue) > 0:
            ((A, B), depth) = Queue.pop(0)
            (P_min, P_max) = self.__P[(A, B)]
            self.__P[(A, B)] = (min(depth, P_min), max(depth, P_max))
            for (C, D) in self.__B:
                if C == B and (C, D) not in BFSMark:
                    BFSMark.append((C, D))
                    Queue.append(((C, D), depth + 1))

        self.__findP_max(relation, 0)

    def end_xes_to_model(self):
        for relation in self.__B:
            self.__F[relation] = sys.maxsize
            self.__P[relation] = (sys.maxsize, -1)

        endState = []
        startState = []

        for caseID, trace in self.__tracelogs.items():
            if trace[-1] not in endState:
                endState.append(trace[-1])
            if trace[0] not in startState:
                startState.append(trace[0])

        for (A, B) in self.__B:
            if A in startState:
                self.__setP((A, B))

        for (A, B) in self.__B:
            if B in endState:
                self.__setF((A, B))
