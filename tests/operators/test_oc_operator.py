import unittest
from pm4py.objects.heuristics_net.obj import HeuristicsNet
from pybeamline.algorithms.discovery.heuristics_miner_lossy_counting import heuristics_miner_lossy_counting
from pybeamline.algorithms.discovery.heuristics_miner_lossy_counting_budget import heuristics_miner_lossy_counting_budget
from pybeamline.algorithms.oc_operator import OCOperator
from pybeamline.sources.dict_ocel_test_source import dict_test_ocel_source


class TestOCOperator(unittest.TestCase):

    def setUp(self):
        # Initialize the OCOperator with a mock control flow
        self.oc_operator_with_control_flow_heuristic = OCOperator(control_flow={
            "Customer": heuristics_miner_lossy_counting(10),
            "Order": heuristics_miner_lossy_counting(11),
            "Item": heuristics_miner_lossy_counting(10),
            "Shipment": heuristics_miner_lossy_counting(10),
        })

        self.oc_operator_with_control_flow_heuristic_budget = OCOperator(control_flow={
            "Customer": heuristics_miner_lossy_counting_budget(10),
            "Order": heuristics_miner_lossy_counting_budget(11),
            "Item": heuristics_miner_lossy_counting_budget(10),
            "Shipment": heuristics_miner_lossy_counting_budget(10),
        })
        self.operator_without_cf = OCOperator()

    def test_oc_operator_mode(self):
       # Check if the operator is initialized correctly with control flow
        self.assertIsInstance(self.oc_operator_with_control_flow_heuristic, OCOperator)
        self.assertIsInstance(self.oc_operator_with_control_flow_heuristic_budget, OCOperator)
        self.assertFalse(self.oc_operator_with_control_flow_heuristic.__dynamic_mode)
        self.assertFalse(self.oc_operator_with_control_flow_heuristic_budget.__dynamic_mode)

        # Check if the operator is initialized correctly without control flow
        self.assertIsInstance(self.operator_without_cf, OCOperator)
        self.assertTrue(self.operator_without_cf.__dynamic_mode)

    def test_oc_operator_with_cf_heuristic_yields_projected_dfg(self):
        # Sample Dict to generate OCEL source
        events = [
            {"activity": "Register Customer", "objects": {"Customer": ["c1"]}},
            {"activity": "Create Order", "objects": {"Customer": ["c1"], "Order": ["o1"]}},
            {"activity": "Add Item", "objects": {"Order": ["o1"], "Item": ["i1"]}},
            {"activity": "Add Item", "objects": {"Order": ["o1"], "Item": ["i2"]}},
            {"activity": "Ship Order", "objects": {"Item": ["i1","i2"], "Order": ["o1"], "Shipment": ["s1"]}},
        ]

        # Generate OCEL source from the events
        ocel_source = dict_test_ocel_source([(events,10)], shuffle=True)
        emitted_models = []
        ocel_source.pipe(
            self.oc_operator_with_control_flow_heuristic.op(),
        ).subscribe(
            on_next=lambda x: emitted_models.append(x),
        )
        # Check if the number of emitted models matches the expected count
        self.assertEqual(len(emitted_models), 10)

        for dictModel in emitted_models:
            # Check if the object type is equal to keys of the control flow
            self.assertIn(dictModel["object_type"], self.oc_operator_with_control_flow_heuristic.__control_flow.keys())
            # Check if the generated model is a HeuristicsNet
            self.assertIsInstance(dictModel["model"], HeuristicsNet)

    def test_oc_operator_without_cf_yields_dfg(self):
        # Sample Dict to generate OCEL source
        events = [
            {"activity": "Register Customer", "objects": {"Customer": ["c1"]}},
            {"activity": "Create Order", "objects": {"Customer": ["c1"], "Order": ["o1"]}},
            {"activity": "Add Item", "objects": {"Order": ["o1"], "Item": ["i1"]}},
            {"activity": "Add Item", "objects": {"Order": ["o1"], "Item": ["i2"]}},
            {"activity": "Ship Order", "objects": {"Item": ["i1","i2"], "Order": ["o1"], "Shipment": ["s1"]}},
        ]
        # Generate OCEL source from the events
        ocel_source = dict_test_ocel_source([(events,50)], shuffle=True) # Default lossy counting parameters Model_update_frequency=10
        emitted_models = []
        ocel_source.pipe(
            self.operator_without_cf.op(),
        ).subscribe(
            on_next=lambda x: emitted_models.append(x),
        )
        # Check if the number of emitted models matches the expected count
        self.assertEqual(len(emitted_models), 11)
        # Check if the control flow is empty
        self.assertEqual(self.operator_without_cf.__control_flow, {})
        for dictModel in emitted_models:
            # Check if the generated model is a HeuristicsNet
            self.assertIsInstance(dictModel["model"], HeuristicsNet)

    def test_oc_operator_with_cf_heuristic_budget_yields_projected_dfg(self):
        events = [
            {"activity": "Register Customer", "objects": {"Customer": ["c1"]}},
            {"activity": "Create Order", "objects": {"Customer": ["c1"], "Order": ["o1"]}},
            {"activity": "Add Item", "objects": {"Order": ["o1"], "Item": ["i1"]}},
            {"activity": "Add Item", "objects": {"Order": ["o1"], "Item": ["i2"]}},
            {"activity": "Ship Order", "objects": {"Item": ["i1", "i2"], "Order": ["o1"], "Shipment": ["s1"]}},
        ]

        # Generate OCEL source from the events
        ocel_source = dict_test_ocel_source([(events, 10)], shuffle=True)
        emitted_models = []
        ocel_source.pipe(
            self.oc_operator_with_control_flow_heuristic_budget.op(),
        ).subscribe(
            on_next=lambda x: emitted_models.append(x),
        )
        # Check if the number of emitted models matches the expected count
        self.assertEqual(len(emitted_models), 10)

        for dictModel in emitted_models:
            # Check if the object type is equal to keys of the control flow
            self.assertIn(dictModel["object_type"], self.oc_operator_with_control_flow_heuristic.__control_flow.keys())
            # Check if the generated model is a HeuristicsNet
            self.assertIsInstance(dictModel["model"], HeuristicsNet)