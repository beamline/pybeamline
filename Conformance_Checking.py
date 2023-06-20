import sys
import graphviz as pgv
from collections import defaultdict
from pybeamline import bevent
from pybeamline.sources import xes_log_source_from_file


class CC_BH:
    def __init__(self, M=([],defaultdict(list),defaultdict(list))):
        # the input is expecting to be the

        self.setM(M) #calls method to set, B, P, and F

        self.__trace_last_event = dict()  # recalls last event for trace to find relation

        self.__conformance = dict()  # saves a traces' conformance
        self.__completeness = dict()  # saves a traces' completeness
        self.__confidence = dict()  # saves a traces' confidence

        self.__obs = defaultdict(list)  # saves all distinct relations in a trace that has ocurred
        self.__inc = dict()  # saves amount of incorrect relations acording to the reference model of a trace

    def __update(self, newEvent):
        caseID = newEvent.get_trace_name()  # easier reference to caseID
        eventName = newEvent.get_event_name()  # easier trefermce to eventNa,e

        if caseID not in self.__trace_last_event:  # if this is first time caseID appears
            self.__trace_last_event[caseID] = eventName  # save current event

            # self.__obs[caseID] = []
            self.__inc[caseID] = 0  # set amount of incorect relations for that CaseId to 0

        else:  # if the caseID has been seen before

            newPattern = (self.__trace_last_event[caseID], eventName)  # locally save the relation
            # Step 1: update internal data structures
            if newPattern in self.__B:  # if the relation is in the aproved relation list
                if newPattern not in self.__obs[caseID]:  # and that relation has not occured for that caseID before
                    self.__obs[caseID].append(newPattern)  # save the relation to that CaseId
            else:  # if the relation is "illegal" acording to B
                self.__inc[caseID] += 1  # increment incorrect for that caseID

            # Step 2: compute online conformance values

            self.__conformance[caseID] = len(self.__obs[caseID]) / (
                        len(self.__obs[caseID]) + self.__inc[caseID])  # calculated conformance

            if newPattern in self.__B:  # if the relation is legal in B
                if self.__P[newPattern][0] <= len(self.__obs[caseID]) and len(self.__obs[caseID]) <= self.__P[newPattern][
                    1]:  # if the relation occurence is within P_min and P_max
                    self.__completeness[caseID] = 1  # set completeness for that caseID
                else:  # if not within P_min and P_max
                    self.__completeness[caseID] = min(1, len(self.__obs[caseID]) / (
                                self.__P[newPattern][0] + 1))  # calculate completeness

                self.__confidence[caseID] = 1 - (self.__F[newPattern] / self.__maxOfMinRelationsAfter)
                # print("case: {0} now has a conformance of: {1}, a completeness of: {2}, and a confidence of: {3}".format(caseID, self.__conformance[caseID],
                #                                                                                       self.__completeness[caseID], self.__confidence[caseID]))
            else:
                self.__confidence[caseID] = -1
                self.__completeness[caseID] = -1
                # print("case: {0} now has a conformance of: {1}, but last behaviour wasnt expected and therefor completeness and confidence cant be found".format(caseID, self.__conformance[caseID]))

            # Step 3: cleanup

            self.__trace_last_event[caseID] = eventName

        # input from algorithm

    def subscribe(self, source):
        source.subscribe(
            on_next=lambda i:
            {
                self.__update(i)
            },
            on_error=lambda e, l: print("Error Occurred: {0} to {1}".format(e, l)),
            on_completed=lambda: {},
        )

    def getConformance(self, caseID = None):
        if caseID == None:
            return self.__conformance
        return self.__conformance[caseID]

    def getCompleteness(self, caseID = None):
        if caseID == None:
            return self.__completeness
        return self.__completeness[caseID]

    def getConfidence(self, caseID = None):
        if caseID == None:
            return self.__confidence
        return self.__confidence[caseID]

    def getM(self):
        return (self.__B, self.__P, self.__F)

    def setM(self, M):
        self.__B = M[0]
        self.__P = M[1]
        self.__F = M[2]
        self.__maxOfMinRelationsAfter = 0
        for relations in self.__F:
            self.__maxOfMinRelationsAfter = max(self.__F[relations], self.__maxOfMinRelationsAfter)

    def getMaxOfF(self):
        return self.__maxOfMinRelationsAfter

    def setM_FromXES(self, fileName):
        self.setM(model_from_XES().set_model_from_XES(fileName))

    def printRefModel(self):
        G = pgv.Graph(comment='ref_model')  # setting type of graph
        G.graph_attr['rankdir'] = 'LR'
        G.node_attr['shape'] = 'box'
        G.edge_attr['arrowType'] = 'normal'

        for (A, B) in self.__B:
            G.edge(A, B, label=str(), dir='forward')

        G.render('model-output/ref_Model.gv').replace('\\', '/')

class model_from_XES(CC_BH):
    def __init__(self):
        self.__B = []
        self.__P = dict()
        self.__F = dict()

        self.__tracelogs = defaultdict(list)

        self.__mark = [] #used to find P_max for all relation
        self.__mark2 = []

    def __update_XES_to_model(self, newEvent):
        caseID = newEvent.get_trace_name()  # locally save caseID
        eventName = newEvent.get_event_name()  # locally save event
        if caseID in self.__tracelogs:  # if this caseID has occured before
            relation = (self.__tracelogs[caseID][-1], eventName)  # find relation based on former event
            traceLength = len(self.__tracelogs[caseID]) - 1  # find the length of the trace so far
            if relation not in self.__B:  # if this relation already exist
                self.__B.append(relation)  # add relation

        else:  # if the trace hasent been seen yet
            self.__tracelogs[caseID] = []

        self.__tracelogs[caseID].append(eventName)  # add the event to the trace log

    def __setF(self, relation):
        Queue = [] #breath first search from an accepting state
        BFSMark = []
        BFSMark.append(relation)
        Queue.append((relation, 0))
        while len(Queue) > 0:
            ((A,B), depth) = Queue.pop(0)
            self.__F[(A,B)] = min(self.__F[(A,B)], depth)
            for (C,D) in self.__B:
                if D == A and (C,D) not in BFSMark:
                    BFSMark.append((C,D))
                    Queue.append(((C,D), depth + 1))

    def __findP_max(self, relation, depth):
        if relation not in self.__mark: #brute force to find longest path
            self.__mark.append(relation)

            (P_min, P_max) = self.__P[relation]
            self.__P[relation] = (P_min, max(depth, P_max))

            (A, B) = relation
            for (C, D) in self.__B:
                if C == B:
                    self.__findP_max((C,D), depth + 1)

            self.__mark.remove(relation)
        else:
            if relation in self.__mark2:
                return
            self.__mark2.append(relation)

            (P_min, P_max) = self.__P[relation]
            self.__P[relation] = (P_min, max(depth, P_max))
            (A, B) = relation
            for (C, D) in self.__B:
                if C == B:
                    self.__findP_max((C,D), depth)

            self.__mark2.remove(relation)

    def __setP(self, relation):
        Queue = [] # breath first state from beggining state
        BFSMark = []
        BFSMark.append(relation)
        Queue.append((relation, 0))
        while len(Queue) > 0:
            ((A,B), depth) = Queue.pop(0)
            (P_min, P_max) = self.__P[(A,B)]
            self.__P[(A,B)] = (min(depth, P_min), max(depth, P_max))
            for (C,D) in self.__B:
                if C == B and (C,D) not in BFSMark:
                    BFSMark.append((C,D))
                    Queue.append(((C,D), depth + 1))

        self.__findP_max(relation, 0)


    def __end_XES_to_model(self):
        for relation in self.__B:
            self.__F[relation] = sys.maxsize
            self.__P[relation] = (sys.maxsize, -1)

        endState = []
        startState = []

        for caseID,trace in self.__tracelogs.items():
            if trace[-1] not in endState:
                endState.append(trace[-1])
            if trace[0] not in startState:
                startState.append(trace[0])

        for (A, B) in self.__B:
            if A in startState:
                self.__setP((A, B))

        for (A,B) in self.__B:
            if B in endState:
                self.__setF((A,B))

    def set_model_from_XES(self, fileName):
        source = xes_log_source_from_file(fileName)
        self.__B = []
        self.__P = dict()
        self.__F = dict()

        self.__tracelogs = dict()

        source.subscribe(
            on_next=lambda i:
            {
                self.__update_XES_to_model(i),
            },
            on_error=lambda e, l: print("Error Occurred: {0} to {1}".format(e, l)),
        )
        self.__end_XES_to_model()

        return (self.__B, self.__P, self.__F)
