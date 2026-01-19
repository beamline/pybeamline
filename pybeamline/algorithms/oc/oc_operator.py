from typing import Dict, Optional, Protocol, Callable, Any, Union, List

from reactivex.subject import Subject

from pybeamline.algorithms.discovery.activity_entity_relation_miner_lossy_counting import activity_entity_relations_miner_lossy_counting
from pybeamline.algorithms.oc.strategies.base import InclusionStrategy, \
    RelativeFrequencyBasedStrategy
from pybeamline.boevent import BOEvent
from pybeamline.algorithms.discovery.heuristics_miner_lossy_counting import heuristics_miner_lossy_counting
from pybeamline.stream.base_map import BaseMap
from pybeamline.stream.base_operator import BaseOperator
from pybeamline.stream.stream import Stream
from reactivex import operators as ops


class StreamMiner(Protocol):
    """
    Protocol representing a callable that consumes a stream of BOEvents and emits process models.
    """
    def __call__(self, stream: Stream[BOEvent]) -> Stream[Any]:
        ... # pragma: no cover

def oc_operator(
    inclusion_strategy: Optional[InclusionStrategy] = None,
    control_flow: Optional[Dict[str, Callable[[], BaseOperator[Stream[Any], Stream[Any]]]]] = None,
    aer_model_update_frequency: Optional[int] = 30,
    aer_model_max_approx_error: Optional[float] = 0.01,
    default_miner: Optional[Callable[[], BaseOperator[Stream[Any], Stream[Any]]]] = None,
) -> BaseMap[BOEvent, dict]:
    """
    Factory function to create a configured OCOperator.
    :param inclusion_strategy:
        Optional object-type concept drift inclusion strategy.
        Determines when object types should be considered active or inactive.
    :param control_flow:
        Optional dictionary mapping object types (str) to miner factory callables (i.e., `Callable[[], StreamMiner]`).
        If omitted, the operator dynamically registers miners for object types on-the-fly.
    :param aer_model_update_frequency:
        Number of events between emissions of updated Activity-Entity Relationship (AER) diagrams.
    :param aer_model_max_approx_error:
        Maximum approximation error used for lossy counting in the AER miner.
        Smaller values reduce approximation error but increase memory usage.
    :param default_miner:
        Optional default miner to use for dynamic mode when no control flow is provided.
        All object types will use the miner specifications provided in the `default_miner` callable.
    :return:
        A callable that takes an `Observable[BOEvent]` as input and returns an `Observable[dict]` containing
        emitted models, AER diagrams, and control commands such as active/inactive.
    """
    if control_flow is not None and not isinstance(control_flow, dict):
        raise TypeError("control_flow must be a dict mapping object types to StreamMiner callables.")
    for obj_type, miner in (control_flow or {}).items():
        if not callable(miner):
            raise ValueError(f"control_flow values must be StreamMiner callables, got {type(miner).__name__} for '{obj_type}'")

    if inclusion_strategy is None:
        inclusion_strategy = RelativeFrequencyBasedStrategy(0.05)

    return OCOperator(
        inclusion_strategy=inclusion_strategy,
        control_flow=control_flow or {},
        aer_model_update_frequency=aer_model_update_frequency,
        aer_model_max_approx_error=aer_model_max_approx_error,
        default_miner=default_miner
    )


class OCOperator(BaseMap[BOEvent, dict]):
    """
    Reactive operator for object-centric process mining, managing multiple per-object-type stream miners and an AER (Activity-Entity Relationship) miner.
    It consumes a stream of BOEvents, dynamically routes and transforms them based on object types, and emits discovered control-flow models
    (DFGs) and AER diagrams at configurable intervals. Supports both static (predefined miners) and dynamic (on-the-fly) miner registration modes,
    and integrates an inclusion strategy to control which object-types to be considered downstream.
    """
    def __init__(self, control_flow: Optional[Dict[str, Callable[[], BaseOperator[Stream[Any], Stream[Any]]]]] = None,
                 inclusion_strategy: InclusionStrategy = None,
                 aer_model_update_frequency: Optional[int] = 30,
                 aer_model_max_approx_error: Optional[float] = 0.01,
                 default_miner: Optional[Callable[[], BaseOperator[Stream[Any], Stream[Any]]]] = None):
        self.__inclusion_strategy = inclusion_strategy or RelativeFrequencyBasedStrategy()
        self.__dynamic_mode = not bool(control_flow)
        self.__control_flow = control_flow or {}
        self.__default_miner = default_miner or (lambda: heuristics_miner_lossy_counting(20))
        self.__miner_subjects: Dict[str, Subject[Union[BOEvent, dict]]] = {}
        self.__output_subject: Subject = Subject()
        self.__output_buffer: List[dict] = []

        for obj_type, miner in self.__control_flow.items():
            self._register_stream(obj_type, miner())
        self._register_aer_stream(aer_model_update_frequency, aer_model_max_approx_error)
        self.__aer_update_frequency = aer_model_update_frequency

    def _register_aer_stream(self, model_update_frequency, max_approx_error: float):
        """
        Register a stream for Activity-Entity Relationship (AER) diagrams using lossy counting.
        """
        # Subject for AER miner input
        aer_subject: Subject = Subject()
        self.__miner_subjects["AERStream"] = aer_subject
        miner_op = activity_entity_relations_miner_lossy_counting(
            model_update_frequency=model_update_frequency,
            control_flow=self.__control_flow,
            max_approx_error=max_approx_error
        )
        # Keep reference to AER mapper to allow synchronous emission checks
        self.__aer_mapper = miner_op
        # Wrap Rx Subject as Stream and run miner, then map to dicts
        aer_stream = Stream(aer_subject).pipe(miner_op).map(lambda model: {
            "type": "aer",
            "model": model
        })
        aer_stream.subscribe(
            on_next=lambda msg: (self.__output_subject.on_next(msg), self.__output_buffer.append(msg)),
            on_error=lambda e: print(f"[AER-STREAM] error:", e),
            on_completed=lambda: None,
            blocking=False
        )

        # Collect output subject emissions into buffer with inclusion strategy
        Stream(self.__output_subject).flat_map(
            lambda item: self.__inclusion_strategy.evaluate(item).to_observable()
        ).subscribe(
            on_next=lambda item: self.__output_buffer.append(item),
            on_error=lambda e: print(f"[OUTPUT] error:", e),
            on_completed=lambda: None,
            blocking=False
        )

    def _register_stream(self, obj_type: str, miner: Optional[BaseOperator[Stream[Any], Stream[Any]]] = None):
        """
        Register a new miner subject for the given object type.
        If a miner is provided, it will be used; otherwise, a default miner is created.
        """
        # Subject per object type for DFG miner input
        obj_subject: Subject = Subject()
        self.__miner_subjects[obj_type] = obj_subject
        miner_op = miner or self.__default_miner()

        # Wrap Rx Subject as Stream and run miner, then map to dicts
        dfg_stream = Stream(obj_subject).pipe(miner_op).map(lambda model: {
            "type": "dfg",
            "object_type": obj_type,
            "model": model
        })

        dfg_stream.subscribe(
            on_next=lambda msg: self.__output_subject.on_next(msg),
            on_error=lambda e: print(f"[{obj_type}] error:", e),
            on_completed=lambda: None,
            blocking=False
        )

    def _route_to_miner(self, event: BOEvent):
        """
        Flatten the incoming BOEvent and route it to its corresponding miner subject.
        If dynamic mode is enabled, miners are created on-the-fly if not present.
        """
        self.__miner_subjects["AERStream"].on_next(event)
        for flat_event in self._flatten(event):
            obj_type = flat_event.get_omap_types()[0]

            if obj_type not in self.__miner_subjects and self.__dynamic_mode:
                # Dynamically create a new miner subject
                self._register_stream(obj_type, self.__default_miner())
            elif obj_type not in self.__miner_subjects:
                continue

            self.__miner_subjects[obj_type].on_next(flat_event)

    def _flatten(self, event: BOEvent) -> List[BOEvent]:
        flattened_events = []
        for obj_type, obj_ids in event.omap.items():
            for obj_id in obj_ids:
                new_omap = {obj_type: {obj_id}}  # Single-entry omap
                flattened_events.append(
                    BOEvent(
                        event_id=event.event_id,
                        activity_name=event.activity_name,
                        timestamp=event.timestamp,
                        omap=new_omap,
                        vmap=event.vmap
                    )
                )
        return flattened_events

    def get_mode(self) -> bool:
        """
        Returns True if the operator is in dynamic mode (no control flow), False otherwise.
        """
        return self.__dynamic_mode

    def transform(self, event: BOEvent) -> Optional[List[dict]]:
        # Route incoming event to miners/AER
        self._route_to_miner(event)

        # Synchronously emit AER model at exact update intervals to avoid races
        try:
            aer_events = self.__aer_mapper.obj_rel.observed_events()
            if self.__aer_update_frequency and aer_events % self.__aer_update_frequency == 0:
                model = self.__aer_mapper.obj_rel.get_model()
                self.__output_buffer.append({"type": "aer", "model": model})
        except Exception:
            pass

        # Drain buffered outputs produced up to now
        if self.__output_buffer:
            results = list(self.__output_buffer)
            self.__output_buffer.clear()
            return results
        return None

    @property
    def operator(self) -> BaseOperator[Stream[BOEvent], Stream[dict]]:
        # Adapter to preserve tests expecting `.operator` producing a Stream operator
        class MapAdapter(BaseOperator[Stream[BOEvent], Stream[dict]]):
            def __init__(self, mapper: BaseMap[BOEvent, dict]):
                self._mapper = mapper

            def apply(self, stream: Stream[BOEvent]) -> Stream[dict]:
                return self._mapper.apply(stream)

        return MapAdapter(self)


