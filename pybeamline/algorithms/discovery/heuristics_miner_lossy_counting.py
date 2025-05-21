import math
from pm4py.algo.discovery.heuristics.variants.classic import calculate as compute_dfg
from pm4py.objects.heuristics_net.obj import HeuristicsNet
from reactivex import just, empty, throw
from reactivex import operators as ops
from reactivex import Observable
from pybeamline.abstractevent import AbstractEvent
from pybeamline.bevent import BEvent
from typing import Callable
from pybeamline.boevent import BOEvent


def heuristics_miner_lossy_counting(
        model_update_frequency=10,
        max_approx_error=0.001,
        dependency_threshold=0.5,
        and_threshold=0.8) -> Callable[[Observable], Observable[HeuristicsNet]]:
    hm = HeuristicsMinerLossyCounting(
        max_approx_error=max_approx_error,
        dependency_threshold=dependency_threshold,
        and_threshold=and_threshold)

    def miner(event: AbstractEvent) -> Observable[HeuristicsNet]:
        if isinstance(event, BOEvent):
            # Verify that the event is flattened
            if len(event.get_object_ids()) != 1:
                raise ValueError("BOEvent should be flattened before supplied to miner")

            # Wrapping BOEvent into BEvent
            trace_name = event.get_object_ids()[0]
           #print(f"[Heurictics Lossy Counting] to ingest event: {event}")
            b_event = BEvent(
                activity_name=event.get_event_name(),
                case_id=trace_name,
                event_time=event.get_event_time()
            )
            hm.ingest_event(b_event)

        elif isinstance(event, BEvent):
            hm.ingest_event(event)
        else:
            raise TypeError(f"Unsupported event type: {type(event)}")

        if hm.observed_events() % model_update_frequency == 0:
            return just(hm.get_model())
        return empty()

    return ops.flat_map(miner)


# Class originally developed by Magnus Frederiksen as part of his BSc project at DTU entitled
# "Development of Process Mining and Complex Event Processing using Python"
class HeuristicsMinerLossyCounting:
    def __init__(self, max_approx_error=0.1, dependency_threshold=0, and_threshold=0.8):
        self.__minimum_dependency_threshold = dependency_threshold  # set dependency threshold to be added to the model
        self.__and_threshold = and_threshold  # set the "and" threshold for when 2 edges leave a node on model

        self.__D_C = dict()  # set of event
        self.__D_R = dict()  # set of relations
        self.__observed_events = 1
        self.__bucket_width = int(math.ceil(1 / max_approx_error))  # set bucket width
        self.__modelRefreshRate = self.__bucket_width  # default model refreshrate

    def ingest_event(self, event):
        current_bucket = int(math.ceil(self.__observed_events / self.__bucket_width))  # calculated bucket

        if event.get_trace_name() in self.__D_C:  # if caseID already exist
            last_event = self.__D_C[event.get_trace_name()]  # localy save former event

            del self.__D_C[event.get_trace_name()]  # replace caseID's former event with new event
            self.__D_C[event.get_trace_name()] = [event.get_event_name(), last_event[1] + 1, last_event[2], event.get_event_time()]

            r_N = (last_event[0], event.get_event_name())  # save relation localy

            if r_N in self.__D_R:  # if relation exists in set
                last_relation = self.__D_R[r_N]  # localy save former relation
                del self.__D_R[r_N]  # replace relation

                diff = (event.get_event_time() - last_event[3]) - last_relation[2]  # calculates the new average relation time
                new_time = last_relation[2] + (diff / (last_relation[0] + 1))

                self.__D_R[r_N] = [last_relation[0] + 1, last_relation[1], new_time]
            else:  # the relation doesent exist, create it
                self.__D_R[r_N] = (1, current_bucket - 1, event.get_event_time() - last_event[3])

        else:  # caseID doesn't exist, create it
            self.__D_C[event.get_trace_name()] = (event.get_event_name(), 1, current_bucket - 1, event.get_event_time())

        # bucket cleaning time
        if self.__observed_events % self.__bucket_width == 0.0:
            # the 2 lists are needed to avoid messing with the coming loops
            D_C_del = []
            D_R_del = []

            for caseID, (eventName, frequency, bucket, time) in self.__D_C.items():
                if frequency + bucket <= current_bucket:  # if not above the error threshold on all events occured
                    D_C_del.append(caseID)

            for caseID in D_C_del:  # deleted the event
                del self.__D_C[caseID]

            for relation, (frequency, bucket, time) in self.__D_R.items():
                if frequency + bucket <= current_bucket:  # if not above the error threshold of all relations
                    D_R_del.append(relation)

            for relation in D_R_del:  # delete the relations
                del self.__D_R[relation]

        self.__observed_events += 1

    def get_model(self):
        dfg = dict()
        for (A, B), (frequency, bucket, time) in self.__D_R.items():
            dfg[(A, B)] = frequency
        hm = HeuristicsNet(dfg)
        return compute_dfg(hm, dependency_thresh=self.__minimum_dependency_threshold, and_measure_thresh=self.__and_threshold)

    def observed_events(self):  # return the total number of events observed
        return self.__observed_events
