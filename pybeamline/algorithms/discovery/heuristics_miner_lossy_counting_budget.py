from typing import Callable
from pm4py.algo.discovery.heuristics.variants.classic import calculate as compute_dfg
from pm4py.objects.heuristics_net.obj import HeuristicsNet
from reactivex import Observable
from reactivex import just, empty
from reactivex import operators as ops
from pybeamline.bevent import BEvent


def heuristics_miner_lossy_counting_budget(
        model_update_frequency=10,
        budget=100,
        dependency_threshold=0.5,
        and_threshold=0.8) -> Callable[[Observable[BEvent]], Observable[HeuristicsNet]]:
    hm = HeuristicsMinerLossyCountingBudget(budget=budget, dependency_threshold=dependency_threshold,
                                            and_threshold=and_threshold)

    def miner(event: BEvent) -> Observable[HeuristicsNet]:
        hm.ingest_event(event)
        if hm.observed_events() % model_update_frequency == 0:
            return just(hm.get_model())
        else:
            return empty()

    return ops.flat_map(miner)


# Class originally developed by Magnus Frederiksen as part of his BSc project at DTU entitled
# "Development of Process Mining and Complex Event Processing using Python"
class HeuristicsMinerLossyCountingBudget:
    def __init__(self, budget=10, dependency_threshold=0, and_threshold=0.8):
        self.__budget = int(budget)  # max length of stored events and relations
        self.__minimum_dependency_threshold = dependency_threshold
        self.__and_threshold = and_threshold

        self.__D_C = dict()  # set of event
        self.__D_R = dict()  # set of relations
        self.__observed_events = 1
        self.__current_bucket = 0

    def ingest_event(self, event):

        # for budget lossy counting, if relation or caseID already exists, the memory will just be replaced and not
        # expanded so we only clean up when the caseID or relation doesnt exist, which results in creating a new one
        if event.get_trace_name() in self.__D_C:  # if caseID already exist
            lastEvent = self.__D_C[event.get_trace_name()]  # localy save former event
            del self.__D_C[event.get_trace_name()]  # replace caseID's former event with new event
            self.__D_C[event.get_trace_name()] = [event.get_event_name(), lastEvent[1] + 1, lastEvent[2],
                                                  event.get_event_time()]

            r_N = (lastEvent[0], event.get_event_name())  # save relation localy

            if r_N in self.__D_R:  # if relation exists in set
                lastRelation = self.__D_R[r_N]  # localy save former relation
                del self.__D_R[r_N]  # replace relation

                diff = (event.get_event_time() - lastEvent[3]) - lastRelation[
                    2]  # incase the user wants the time for each relation
                newTime = lastRelation[2] + (diff / (lastRelation[0] + 1))

                self.__D_R[r_N] = [lastRelation[0] + 1, lastRelation[1], newTime]

            else:  # the relation doesent exist, create it
                while len(self.__D_R) + len(
                        self.__D_C) >= self.__budget:  # if budget is reached when adding a new key + iten
                    self.__bucket_cleaning()  # bucket cleaning time
                self.__D_R[r_N] = (1, self.__current_bucket, event.get_event_time() - lastEvent[3])

        else:  # caseID doesnt exist, create it
            while ((len(self.__D_R) + len(
                    self.__D_C)) >= self.__budget):  # if budget is reached when adding a new key + iten
                self.__bucket_cleaning()  # bucket cleaning time
            self.__D_C[event.get_trace_name()] = (
            event.get_event_name(), 1, self.__current_bucket, event.get_event_time())

        # clean up
        self.__observed_events += 1

    def __bucket_cleaning(self):
        self.__current_bucket += 1  # increment bucket to clean all items not within the new bucket number

        D_Ctobedel = []  # the 2 lists are needed to avoid messing with the comming loops
        D_Rtobedel = []

        for caseID, (eventName, frequency, bucket, time) in self.__D_C.items():  # for all caseIDs' occured
            if frequency + bucket <= self.__current_bucket:  # if not above the bucket threshold on all events occured
                D_Ctobedel.append(caseID)

        for caseID in D_Ctobedel:  # deleted the event
            del self.__D_C[caseID]

        for relation, (frequency, bucket, time) in self.__D_R.items():  # for all relations occured
            if frequency + bucket <= self.__current_bucket:  # if not above the bucket threshold of all relations
                D_Rtobedel.append(relation)

        for relation in D_Rtobedel:  # delete the relations
            del self.__D_R[relation]

    def get_model(self):
        dfg = dict()
        for (A, B), (frequency, bucket, time) in self.__D_R.items():
            dfg[(A, B)] = frequency
        hm = HeuristicsNet(dfg)
        return compute_dfg(hm, dependency_thresh=self.__minimum_dependency_threshold,
                           and_measure_thresh=self.__and_threshold)

    def observed_events(self):  # returns map of events observed
        return self.__observed_events - 1
