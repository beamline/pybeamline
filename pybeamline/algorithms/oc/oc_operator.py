from typing import Dict, Optional, Protocol, Callable, Any, Union, Set
from reactivex import operators as ops, Observable, merge, empty, just, from_iterable
from reactivex.subject import Subject

from pybeamline.algorithms.discovery.activity_object_relation_miner_lossy_counting import \
    activity_object_relations_miner_lossy_counting
from pybeamline.boevent import BOEvent
from pybeamline.algorithms.discovery.heuristics_miner_lossy_counting import heuristics_miner_lossy_counting
from pybeamline.utils.commands import create_command, Command


class StreamMiner(Protocol):
    """
    Protocol representing a callable that consumes a stream of BOEvents and emits HeuristicsNet models.
    """
    def __call__(self, stream: Observable[BOEvent]) -> Observable[Any]:
        ...


def oc_operator(
    control_flow: Optional[Dict[str, Callable[[], StreamMiner]]] = None,
    frequency_threshold: float = 0.05,
    aer_model_update_frequency: int = 30,
    aer_model_max_approx_error: float = 0.01
) -> Callable[[Observable[BOEvent]], Observable[dict]]:
    """
    Factory function to create a configured OCOperator.
    :param control_flow:
        Optional dictionary mapping object types (str) to miner factory callables (i.e., `Callable[[], StreamMiner]`).
        If omitted, the operator dynamically registers miners for object types on-the-fly.
    :param frequency_threshold:
        A float (e.g., 0.05) specifying the minimum relative frequency of obj-type miner outputs required to register an object type.
        If the proportion of emitted models for an object type falls below this threshold, it will be deregistered.
    :param aer_model_update_frequency:
        Number of events between emissions of updated Activity-Entity Relationship (AER) diagrams.
    :param aer_model_max_approx_error:
        Maximum approximation error used for lossy counting in the AER miner.
        Smaller values reduce approximation error but increase memory usage.
    :return:
        A callable that takes an `Observable[BOEvent]` as input and returns an `Observable[dict]` containing
        emitted models, AER diagrams, and control commands such as register/deregister.
    """
    if control_flow is not None and not isinstance(control_flow, dict):
        raise TypeError("control_flow must be a dict mapping object types to StreamMiner callables.")
    for obj_type, miner in (control_flow or {}).items():
        if not callable(miner):
            raise ValueError(f"control_flow values must be StreamMiner callables, got {type(miner).__name__} for '{obj_type}'")

    return OCOperator(
        control_flow=control_flow or {},
        frequency_threshold=frequency_threshold,
        aer_model_update_frequency=aer_model_update_frequency,
        aer_model_max_approx_error=aer_model_max_approx_error
    ).operator


class OCOperator:
    """
    Object-Centric Reactive Operator for managing obj-type stream miners for processing of BOEvents.
    Dynamically registers/deregisters miners based on model emission frequencies.
    """
    def __init__(self, control_flow: Optional[Dict[str, Callable[[], StreamMiner]]], frequency_threshold: float = 0.05,aer_model_update_frequency: int = 30, aer_model_max_approx_error: float = 0.01):
        self.__frequency_threshold = int(1/frequency_threshold)
        self.__control_flow = control_flow
        self.__dynamic_mode = not bool(control_flow)

        self.__miner_subjects: Dict[str, Subject[Union[BOEvent, dict]]] = {}
        self.__output_subject: Subject = Subject()

        self._emitted_models: Dict[str, int] = {}
        self._total_emitted_models: int = 0
        self._registered_object_types: Set[str] = set()


        for obj_type, miner in control_flow.items():
            self._register_stream(obj_type, miner())
        self._register_aer_stream(aer_model_update_frequency, aer_model_max_approx_error)

    def _register_aer_stream(self, model_update_frequency, max_approx_error: float):
        subject = Subject[BOEvent]()
        self.__miner_subjects["AERStream"] = subject
        miner_op = activity_object_relations_miner_lossy_counting(
            model_update_frequency=model_update_frequency,
            control_flow=self.__control_flow,
            max_approx_error=max_approx_error
        )
        aer_stream = subject.pipe(
            miner_op,
            ops.map(lambda model: {
                "type": "aer_diagram",
                "model": model
            }),
        )
        aer_stream.subscribe(
            on_next=lambda msg: self.__output_subject.on_next(msg),
            on_error=lambda e: print(f"[AER-STREAM] error:", e),
            on_completed=lambda: None
        )


    def _register_stream(self, obj_type: str, miner: Optional[StreamMiner] = None):
        """
        Register a new miner stream for the given object type.
        """
        subject = Subject[BOEvent]()
        self.__miner_subjects[obj_type] = subject
        miner_op = miner or heuristics_miner_lossy_counting(20)

        dfg_stream = subject.pipe(
            miner_op,
            ops.map(lambda model: {
                "type": "model",
                "object_type": obj_type,
                "model": model
            }),
        )

        dfg_stream.subscribe(
            on_next=lambda msg: self.__output_subject.on_next(msg),
            on_error=lambda e: print(f"[{obj_type}] error:", e),
            on_completed=lambda: None
        )

    def _route_to_miner(self, event: BOEvent):
        """
        Flatten the incoming BOEvent and route it to its corresponding miner subject.
        If dynamic mode is enabled, miners are created on-the-fly if not present.
        """
        self.__miner_subjects["AERStream"].on_next(event)
        for flat_event in event.flatten():
            obj_type = flat_event.get_omap_types()[0]

            if obj_type not in self.__miner_subjects and (self.__dynamic_mode or obj_type in self.__control_flow):
                if obj_type in self.__control_flow:
                    # Reregistration of selected miner in control flow
                    self._register_stream(obj_type, miner=self.__control_flow[obj_type]())
                else:
                    # Dynamically create a new miner subject
                    self._register_stream(obj_type)

            if obj_type not in self.__miner_subjects:
                continue
            self.__miner_subjects[obj_type].on_next(flat_event)

    def _evaluate_model_frequency(self, event: dict) -> Observable[dict]:
        if event.get("type") != "model":
            return just(event)

        obj_type = event["object_type"]
        self._increment_obj_type_model_count(obj_type)

        commands_and_model = []

        # Add commands before the model
        register_cmds = self._evaluate_registration(obj_type)
        if register_cmds:
            commands_and_model.extend(register_cmds)
        dereg_cmds = self._evaluate_deregistration_all()
        if dereg_cmds:
            commands_and_model.extend(dereg_cmds)
        # Append the actual model event last
        commands_and_model.append(event)
        return from_iterable(commands_and_model)

    def _increment_obj_type_model_count(self, obj_type: str):
        self._emitted_models[obj_type] = self._emitted_models.get(obj_type, 0) + 1
        self._total_emitted_models += 1

    def _evaluate_registration(self, obj_type: str):
        count = self._emitted_models.get(obj_type, 0)
        total = self._total_emitted_models
        if self.__frequency_threshold * count >= total and obj_type not in self._registered_object_types:
            self._registered_object_types.add(obj_type)
            return [create_command(Command.REGISTER, obj_type)]

    def _evaluate_deregistration_all(self):
        total = self._total_emitted_models
        commands = []
        for obj_type in list(self._registered_object_types):
            count = self._emitted_models.get(obj_type, 0)
            if self.__frequency_threshold * count < total:
                self._registered_object_types.remove(obj_type)
                commands.append(create_command(Command.DEREGISTER, obj_type))
        return commands


    def get_mode(self) -> bool:
        """
        Returns True if the operator is in dynamic mode (no control flow), False otherwise.
        """
        return self.__dynamic_mode

    @property
    def operator(self) -> Callable[[Observable[BOEvent]], Observable[dict]]:
        def pipeline(stream: Observable[BOEvent]) -> Observable[dict]:
            return stream.pipe(
                ops.do_action(self._route_to_miner),
                ops.filter(lambda e: not isinstance(e, BOEvent)),
                ops.merge(self.__output_subject),
                ops.flat_map(self._evaluate_model_frequency),
            )
        return pipeline
