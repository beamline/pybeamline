import unittest
from typing import Any

from pybeamline.algorithms.discovery.oc_heuristics_miner_lossy_counting import oc_heuristics_miner_lossy_counting
from pybeamline.algorithms.oc_operator import OCOperator
from pybeamline.algorithms.ocdfg_merge_operator import ocdfg_merge_operator
from pybeamline.sources.dict_ocel_test_source import dict_test_ocel_source
from pybeamline.utils.ocdfg_merger import OCDFGMerger



class TestOCDFGMergeOperator(unittest.TestCase):

    def setUp(self):
        test_events_phaseflow = [
            {"activity": "Register Customer", "objects": {"Customer": ["c1"]}},
            {"activity": "Create Order", "objects": {"Customer": ["c1"], "Order": ["o1"]}},
            {"activity": "Add Item", "objects": {"Order": ["o1"], "Item": ["i1"]}},
            {"activity": "Reserve Item", "objects": {"Item": ["i1"]}},
            {"activity": "Pack Item", "objects": {"Item": ["i1"], "Order": ["o1"]}},
            {"activity": "Ship Item", "objects": {"Item": ["i1"], "Shipment": ["s1"]}},
            {"activity": "Send Invoice", "objects": {"Order": ["o1"], "Invoice": ["inv1"]}},
            {"activity": "Receive Review", "objects": {"Customer": ["c1"], "Order": ["o1"]}},
        ]

        test_events_phaseflow_ends_early = [
            {"activity": "Register Customer", "objects": {"Customer": ["c2"]}},
            {"activity": "Create Order", "objects": {"Customer": ["c2"], "Order": ["o2"]}},
            {"activity": "Add Item", "objects": {"Order": ["o2"], "Item": ["i2"]}},
            {"activity": "Reserve Item", "objects": {"Item": ["i2"]}},
            {"activity": "Cancel Order", "objects": {"Customer": ["c2"], "Order": ["o2"]}}
        ]
        self.combined_log = dict_test_ocel_source(
            [(test_events_phaseflow_ends_early, 25), (test_events_phaseflow, 2500)],
            shuffle=False
        )
        control_flow = {
            "Order": oc_heuristics_miner_lossy_counting(model_update_frequency=10, max_approx_error=0.1),
            "Item": oc_heuristics_miner_lossy_counting(model_update_frequency=10),
            "Customer": oc_heuristics_miner_lossy_counting(model_update_frequency=10, max_approx_error=0.1),
            "Shipment": oc_heuristics_miner_lossy_counting(model_update_frequency=1),
            "Invoice": oc_heuristics_miner_lossy_counting(model_update_frequency=1),
        }
        self.oc_operator = OCOperator(control_flow)
        self.oc_merger = OCDFGMerger()

    def test_ocdfg_merger(self):
        # Test the OCDFG merger with the combined log
        emitted_models = []
        self.combined_log.pipe(
            self.oc_operator.op(),
            ocdfg_merge_operator()
        ).subscribe(lambda merged_ocdfg: emitted_models.append(merged_ocdfg))

        for merged_ocdfg in emitted_models:
            # No empty models should be emitted
            self.assertTrue(len(merged_ocdfg) > 0)
            # Check that the merged model contains the expected object types
            for tuple in merged_ocdfg:
                self.assertIn(tuple[1], ["Order", "Item", "Customer", "Shipment", "Invoice"])

