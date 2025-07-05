import unittest
from reactivex import operators as ops
from pybeamline.algorithms.discovery import heuristics_miner_lossy_counting_budget
from pybeamline.algorithms.discovery.heuristics_miner_lossy_counting import heuristics_miner_lossy_counting
from pybeamline.algorithms.oc.oc_operator import OCOperator, oc_operator
from pybeamline.algorithms.oc.oc_merge_operator import oc_merge_operator
from pybeamline.algorithms.oc.strategies.base import RelativeFrequencyBasedStrategy
from pybeamline.sources.dict_ocel_test_source import dict_test_ocel_source
from pybeamline.algorithms.oc.oc_merge_operator import OCMergeOperator


class TestOCMergeOperator(unittest.TestCase):

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
            {"activity": "Add Item", "objects": {"Order": ["o2"], "Item": ["i2"]}},
            {"activity": "Reserve Item", "objects": {"Item": ["i2"]}},
            {"activity": "Cancel Order", "objects": {"Customer": ["c2"], "Order": ["o2"]}}
        ]

        test_events_other_workflow = [
            {"activity": "Register Guest", "objects": {"Guest": ["g1"]}},
            {"activity": "Create Booking", "objects": {"Guest": ["g1"], "Booking": ["b1"]}},
            {"activity": "Reserve Room", "objects": {"Booking": ["b1"]}},
            {"activity": "Check In", "objects": {"Guest": ["g1"], "Booking": ["b1"]}},
            {"activity": "Check Out", "objects": {"Guest": ["g1"], "Booking": ["b1"]}}
        ]

        self.combined_log = dict_test_ocel_source(
            [(test_events_phaseflow_ends_early, 5), (test_events_phaseflow, 25)],
            shuffle=False)

        self.combined_log_two_workflows = dict_test_ocel_source([(test_events_phaseflow_ends_early,10), (test_events_other_workflow,40)], shuffle=False)

        control_flow = {
            "Order": lambda : heuristics_miner_lossy_counting(model_update_frequency=10, max_approx_error=0.1),
            "Item": lambda : heuristics_miner_lossy_counting(model_update_frequency=10),
            "Customer": lambda : heuristics_miner_lossy_counting(model_update_frequency=10, max_approx_error=0.1),
            "Shipment": lambda : heuristics_miner_lossy_counting(model_update_frequency=1),
            "Invoice": lambda : heuristics_miner_lossy_counting(model_update_frequency=1),
        }
        self.oc_operator = OCOperator(inclusion_strategy=RelativeFrequencyBasedStrategy(frequency_threshold=0.15), control_flow=control_flow)
        self.oc_operator_with_budget = OCOperator(control_flow={
            "Order": lambda : heuristics_miner_lossy_counting_budget(model_update_frequency=10),
            "Item": lambda : heuristics_miner_lossy_counting_budget(model_update_frequency=10),
            "Customer": lambda : heuristics_miner_lossy_counting_budget(model_update_frequency=10),
            "Shipment": lambda : heuristics_miner_lossy_counting_budget(model_update_frequency=1),
            "Invoice": lambda : heuristics_miner_lossy_counting_budget(model_update_frequency=1),
        })
        self.oc_merger = OCMergeOperator()

    def test_oc_merger_with_heuristic(self):
        # Test the OCDFG merger with the combined log
        emitted_models = []
        self.combined_log.pipe(
            self.oc_operator.operator,
            oc_merge_operator()
        ).subscribe(lambda merged_ocdfg: emitted_models.append(merged_ocdfg["ocdfg"]))

        for merged_ocdfg in emitted_models:
            if merged_ocdfg is None or not merged_ocdfg.edges:
                continue
            # No empty models should be emitted
            self.assertTrue(len(merged_ocdfg.edges.keys()) > 0)
            self.assertTrue(len(merged_ocdfg.activities) > 0)

            # Check if the merged model contains the expected activities
            for activity in merged_ocdfg.activities:
                self.assertIn(activity, {"Register Customer", "Create Order",
                                         "Add Item", "Reserve Item", "Cancel Order",
                                         "Pack Item", "Ship Item", "Send Invoice", "Receive Review"})

    def test_oc_merger_with_heuristic_budget(self):
        # Test the OCDFG merger with the combined log
        emitted_models = []
        self.combined_log.pipe(
            self.oc_operator_with_budget.operator,
            oc_merge_operator(),
            ops.filter(lambda x: x.get("ocdfg") is not None),
        ).subscribe(lambda merged_ocdfg: emitted_models.append(merged_ocdfg["ocdfg"]))

        for merged_ocdfg in emitted_models:
            if merged_ocdfg is None or not merged_ocdfg.edges:
                continue
            # No empty models should be emitted
            self.assertTrue(len(merged_ocdfg.edges) > 0)
            self.assertTrue(len(merged_ocdfg.activities) > 0)

            # Check if the merged model contains the expected activities
            for activity in merged_ocdfg.activities:
                self.assertIn(activity, {"Register Customer", "Create Order",
                                         "Add Item", "Reserve Item", "Cancel Order",
                                         "Pack Item", "Ship Item", "Send Invoice", "Receive Review"})


    def test_oc_merger_with_emit_frequency_two_workflows(self):
        emitted_models = []
        self.combined_log_two_workflows.pipe(
            oc_operator(inclusion_strategy=RelativeFrequencyBasedStrategy(frequency_threshold=0.02)),
            oc_merge_operator()
        ).subscribe(lambda merged_ocdfg: emitted_models.append(merged_ocdfg["ocdfg"]))

        self.assertTrue({"Guest", "Booking"}.issubset(emitted_models[-1].object_types))
        self.assertTrue({"Customer", "Order", "Item"}.issubset(emitted_models[6].object_types))
        self.assertIn("OCDFG:", emitted_models[6].__str__())
        repr_str = repr(emitted_models[-1])
        self.assertIn("OCDFG", repr_str)
        self.assertIn("activities", repr_str)
        self.assertIn('Guest', repr_str)
        self.assertIn("edges=", repr_str)

    def test_oc_merger_handles_aer_diagram_correctly(self):
        emitted_aer_diagrams = []
        self.combined_log_two_workflows.pipe(
            oc_operator(aer_model_max_approx_error=0.01, aer_model_update_frequency=30),
            oc_merge_operator(),
        ).subscribe(lambda merged_ocdfg: emitted_aer_diagrams.append(merged_ocdfg["aer"]))

        # Verify that workflow 1 are in the beginning of the emitted AER diagrams

        self.assertTrue(len(emitted_aer_diagrams) > 0)
        
        # Get removes all entries where relations are empty
        emitted_aer_diagrams = [aer for aer in emitted_aer_diagrams if aer.relations]
        self.assertIn("Customer", emitted_aer_diagrams[0].get_object_types("Register Customer"))
        self.assertTrue({'Create Order', 'Register Customer', 'Add Item', 'Cancel Order'}.issubset(emitted_aer_diagrams[0].get_activities()))
        self.assertTrue({"Create Booking", "Check In", "Check Out", "Register Guest", "Reserve Room"}.issubset(emitted_aer_diagrams[-1].get_activities()))

