import unittest
from reactivex import operators as ops
from pybeamline.algorithms.discovery import heuristics_miner_lossy_counting_budget
from pybeamline.algorithms.discovery.heuristics_miner_lossy_counting import heuristics_miner_lossy_counting
from pybeamline.algorithms.oc.oc_operator import OCOperator
from pybeamline.algorithms.oc.ocdfg_merge_operator import ocdfg_merge_operator
from pybeamline.sources.dict_ocel_test_source import dict_test_ocel_source
from pybeamline.algorithms.oc.ocdfg_merge_operator import OCDFGMerger



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
            "Order": heuristics_miner_lossy_counting(model_update_frequency=10, max_approx_error=0.1),
            "Item": heuristics_miner_lossy_counting(model_update_frequency=10),
            "Customer": heuristics_miner_lossy_counting(model_update_frequency=10, max_approx_error=0.1),
            "Shipment": heuristics_miner_lossy_counting(model_update_frequency=1),
            "Invoice": heuristics_miner_lossy_counting(model_update_frequency=1),
        }
        self.oc_operator = OCOperator(control_flow)
        self.oc_operator_with_budget = OCOperator(control_flow={
            "Order": heuristics_miner_lossy_counting_budget(model_update_frequency=10),
            "Item": heuristics_miner_lossy_counting_budget(model_update_frequency=10),
            "Customer": heuristics_miner_lossy_counting_budget(model_update_frequency=10),
            "Shipment": heuristics_miner_lossy_counting_budget(model_update_frequency=1),
            "Invoice": heuristics_miner_lossy_counting_budget(model_update_frequency=1),
        })
        self.oc_merger = OCDFGMerger()

    def test_ocdfg_merger_with_heuristic(self):
        # Test the OCDFG merger with the combined log
        emitted_models = []
        self.combined_log.pipe(
            self.oc_operator.op(),
            ocdfg_merge_operator()
        ).subscribe(lambda merged_ocdfg: emitted_models.append(merged_ocdfg))

        for merged_ocdfg in emitted_models:
            # No empty models should be emitted
            self.assertTrue(len(merged_ocdfg["ocdfg"].edges.keys()) > 0)
            self.assertTrue(len(merged_ocdfg["ocdfg"].activities) > 0)

            # Check if the merged model contains the expected activities
            for activity in merged_ocdfg["ocdfg"].activities:
                self.assertIn(activity, {"Register Customer", "Create Order",
                                         "Add Item", "Reserve Item", "Cancel Order",
                                         "Pack Item", "Ship Item", "Send Invoice", "Receive Review"})

    def test_ocdfg_merger_with_heuristic_budget(self):
        # Test the OCDFG merger with the combined log
        emitted_models = []
        self.combined_log.pipe(
            self.oc_operator_with_budget.op(),
            ocdfg_merge_operator(),
            ops.filter(lambda x: x.get("ocdfg") is not None),
        ).subscribe(lambda merged_ocdfg: emitted_models.append(merged_ocdfg))

        for output in emitted_models:
            merged_ocdfg = output["ocdfg"]
            # No empty models should be emitted
            self.assertTrue(len(merged_ocdfg.edges) > 0)
            self.assertTrue(len(merged_ocdfg.activities) > 0)

            # Check if the merged model contains the expected activities
            for activity in merged_ocdfg.activities:
                self.assertIn(activity, {"Register Customer", "Create Order",
                                         "Add Item", "Reserve Item", "Cancel Order",
                                         "Pack Item", "Ship Item", "Send Invoice", "Receive Review"})


