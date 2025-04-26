from typing import Callable
from reactivex import operators as ops, Observable

from pybeamline.utils.ocdfg_merger import OCDFGMerger


def ocdfg_merge_operator(merger: OCDFGMerger = None) -> Callable[[Observable], Observable]:
    """
    Reactive operator that merges incoming object-type DFG models
    into a ODFM structure, using OCDFGMerger.

    :param merger: Optional pre-initialized OCDFGMerger
    :return: RxPy operator (function) which is a MergedOCDFG
    """
    if merger is None:
        merger = OCDFGMerger()

    return lambda stream: stream.pipe(
        ops.filter(lambda model_dict: merger.should_update(model_dict["object_type"], model_dict["model"])),
        ops.map(lambda model_dict: merger.merge(model_dict["object_type"], model_dict["model"]))
    )