from typing import Callable
from reactivex import operators as ops, Observable

from pybeamline.utils.ocdfg_merger import OCDFGMerger


def ocdfg_merge_operator(merger: OCDFGMerger = None) -> Callable[[Observable], Observable]:
    """
    Reactive operator that merges incoming object-type DFG models
    into a ODFM structure, using OCDFGMerger.

    :param merger: Optional pre-initialized OCDFGMerger
    :return: RxPy operator (function)
    """
    if merger is None:
        merger = OCDFGMerger()

    def _merge_if_changed(model_dict):
        obj_type = model_dict["object_type"]
        model = model_dict["model"]
        if merger.should_update(obj_type, model):
            merger.merge(model_dict)
            return model_dict  # only emit if changed
        return None  # filtered out



    return lambda stream: stream.pipe(
        ops.map(_merge_if_changed),
        ops.filter(lambda x: x is not None)
    )