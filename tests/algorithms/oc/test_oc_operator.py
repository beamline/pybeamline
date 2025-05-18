import unittest
from pm4py.objects.heuristics_net.obj import HeuristicsNet
from pybeamline.algorithms.discovery.heuristics_miner_lossy_counting import heuristics_miner_lossy_counting
from pybeamline.algorithms.discovery.heuristics_miner_lossy_counting_budget import heuristics_miner_lossy_counting_budget
from pybeamline.algorithms.oc.oc_operator import OCOperator, oc_operator
from pybeamline.sources.dict_ocel_test_source import dict_test_ocel_source


class TestOCOperator(unittest.TestCase):

    def setUp(self):
        # Initialize the OCOperator with a mock control flow
        self.oc_operator_with_control_flow_heuristic = OCOperator(control_flow={
            "Customer": heuristics_miner_lossy_counting(10),
            "Order": heuristics_miner_lossy_counting(10),
            "Item": heuristics_miner_lossy_counting(10),
            "Shipment": heuristics_miner_lossy_counting(10),
        })

        self.oc_operator_with_control_flow_heuristic_budget = OCOperator(control_flow={
            "Customer": heuristics_miner_lossy_counting_budget(10),
            "Order": heuristics_miner_lossy_counting_budget(10),
            "Item": heuristics_miner_lossy_counting_budget(10),
            "Shipment": heuristics_miner_lossy_counting_budget(10),
        })
        self.operator_without_cf = OCOperator(control_flow={})

    def test_oc_operator_mode(self):
       # Check if the operator is initialized correctly with control flow
        self.assertIsInstance(self.oc_operator_with_control_flow_heuristic, OCOperator)
        self.assertIsInstance(self.oc_operator_with_control_flow_heuristic_budget, OCOperator)
        self.assertFalse(self.oc_operator_with_control_flow_heuristic.get_mode())
        self.assertFalse(self.oc_operator_with_control_flow_heuristic_budget.get_mode())

        # Check if the operator is initialized correctly without control flow
        self.assertIsInstance(self.operator_without_cf, OCOperator)
        self.assertTrue(self.operator_without_cf.get_mode())

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
        self.assertEqual(10, len(emitted_models),)

        for dictModel in emitted_models:
            # Check if the object type is equal to keys of the control flow
            self.assertIn(dictModel["object_type"], self.oc_operator_with_control_flow_heuristic.get_control_flow().keys())
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
        self.assertEqual( 10,len(emitted_models))
        # Check if the control flow is empty
        self.assertEqual(0, len(self.operator_without_cf.get_control_flow().keys()))
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
        self.assertEqual(10, len(emitted_models))

        for dictModel in emitted_models:
            # Check if the object type is equal to keys of the control flow
            self.assertIn(dictModel["object_type"], self.oc_operator_with_control_flow_heuristic.get_control_flow().keys())
            # Check if the generated model is a HeuristicsNet
            self.assertIsInstance(dictModel["model"],HeuristicsNet)

    def test_control_flow_input_errors(self):
        events = [
            {"activity": "Register Customer", "objects": {"Customer": ["c1"]}},
            {"activity": "Create Order", "objects": {"Customer": ["c1"], "Order": ["o1"]}},
            {"activity": "Add Item", "objects": {"Order": ["o1"], "Item": ["i1"]}},
            {"activity": "Add Item", "objects": {"Order": ["o1"], "Item": ["i2"]}},
            {"activity": "Ship Order", "objects": {"Item": ["i1", "i2"], "Order": ["o1"], "Shipment": ["s1"]}},
        ]
        # Generate OCEL source from the events
        ocel_source = dict_test_ocel_source([(events, 10)], shuffle=True)
        cf_not_correct_type = False
        cf_not_correct_values = {
            "Customer": False
        }

        try:
            ocel_source.pipe(
                oc_operator(control_flow=cf_not_correct_type)
            ).subscribe()
        except TypeError as e:
            self.assertEqual("control_flow must be a dict mapping object types to StreamMiner callables.",str(e))

        try:
            ocel_source.pipe(
                oc_operator(control_flow=cf_not_correct_values)
            ).subscribe()
        except ValueError as e:
            self.assertEqual("control_flow values must be StreamMiner callables, got bool for 'Customer'", str(e))
