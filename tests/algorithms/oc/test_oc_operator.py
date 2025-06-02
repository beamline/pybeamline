import unittest
from pm4py.objects.heuristics_net.obj import HeuristicsNet
from pybeamline.algorithms.discovery.heuristics_miner_lossy_counting import heuristics_miner_lossy_counting
from pybeamline.algorithms.discovery.heuristics_miner_lossy_counting_budget import heuristics_miner_lossy_counting_budget
from pybeamline.utils.commands import Command
from pybeamline.algorithms.oc.oc_operator import OCOperator, oc_operator
from pybeamline.objects.aer_diagram import ActivityERDiagram
from pybeamline.sources.dict_ocel_test_source import dict_test_ocel_source
from reactivex import operators as ops

from pybeamline.utils.cardinality import Cardinality


class TestOCOperator(unittest.TestCase):

    def setUp(self):
        # Initialize the OCOperator with a mock control flow
        self.oc_operator_with_control_flow_heuristic = OCOperator(control_flow={
            "Customer": lambda : heuristics_miner_lossy_counting(10),
            "Order": lambda : heuristics_miner_lossy_counting(10),
            "Item": lambda : heuristics_miner_lossy_counting(10),
            "Shipment": lambda : heuristics_miner_lossy_counting(10),
        })

        self.oc_operator_with_control_flow_heuristic_budget = OCOperator(control_flow={
            "Customer": lambda : heuristics_miner_lossy_counting_budget(10),
            "Order": lambda :heuristics_miner_lossy_counting_budget(10),
            "Item": lambda : heuristics_miner_lossy_counting_budget(10),
            "Shipment": lambda : heuristics_miner_lossy_counting_budget(10),
        })
        self.operator_without_cf = OCOperator(control_flow={})

        self.events = [
            {"activity": "Register Customer", "objects": {"Customer": ["c1"]}},
            {"activity": "Create Order", "objects": {"Customer": ["c1"], "Order": ["o1"]}},
            {"activity": "Add Item", "objects": {"Order": ["o1"], "Item": ["i1"]}},
            {"activity": "Add Item", "objects": {"Order": ["o1"], "Item": ["i2"]}},
            {"activity": "Ship Order", "objects": {"Item": ["i1","i2"], "Order": ["o1"], "Shipment": ["s1"]}},
        ]

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
        # Generate OCEL source from the events
        ocel_source = dict_test_ocel_source([(self.events,10)], shuffle=True)
        emitted_models = []
        ocel_source.pipe(
            self.oc_operator_with_control_flow_heuristic.operator,
            ops.filter(lambda output: output["type"] == "model" and isinstance(output["model"], HeuristicsNet))
        ).subscribe(
            on_next=lambda x: emitted_models.append(x),
        )
        # Check if the number of emitted models matches the expected count
        self.assertEqual(11, len(emitted_models),)

        for dictModel in emitted_models:
            # Check if the generated model is a HeuristicsNet
            self.assertIsInstance(dictModel["model"], HeuristicsNet)

    def test_oc_operator_without_cf_yields_dfg(self):
        # Generate OCEL source from the events
        ocel_source = dict_test_ocel_source([(self.events,50)], shuffle=True) # Default lossy counting parameters Model_update_frequency=10
        emitted_models = []
        ocel_source.pipe(
            self.operator_without_cf.operator,
            ops.filter(lambda output: output["type"] == "model" and isinstance(output["model"], HeuristicsNet))
        ).subscribe(
            on_next=lambda x: emitted_models.append(x),
        )
        # Check if the number of emitted models matches the expected count
        self.assertEqual( 27,len(emitted_models))
        # Check if the control flow is empty aka. Dynamic mode is enabled
        self.assertEqual(True, self.operator_without_cf.get_mode())
        for dictModel in emitted_models:
            # Check if the generated model is a HeuristicsNet
            self.assertIsInstance(dictModel["model"], HeuristicsNet)

    def test_oc_operator_with_cf_heuristic_budget_yields_projected_dfg(self):
        # Generate OCEL source from the events
        ocel_source = dict_test_ocel_source([(self.events, 10)], shuffle=True)
        emitted_models = []
        ocel_source.pipe(
            self.oc_operator_with_control_flow_heuristic_budget.operator,
            ops.filter(lambda output: output["type"] == "model" and isinstance(output["model"], HeuristicsNet))
        ).subscribe(
            on_next=lambda x: emitted_models.append(x),
        )
        # Check if the number of emitted models matches the expected count
        # Customer = 2*10 = 2, # Order = 4*10 = 4, # Item = 4*10 = 4, # Shipment = 1*10 = 1 # Sum = 2 + 4 + 4 + 1 = 11
        self.assertEqual(11, len(emitted_models))

        for dictModel in emitted_models:
            # Check if the generated model is a HeuristicsNet
            self.assertIsInstance(dictModel["model"],HeuristicsNet)

    def test_control_flow_input_errors(self):
        # Generate OCEL source from the events
        ocel_source = dict_test_ocel_source([(self.events, 10)], shuffle=True)
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

    def test_oc_operator_with_object_max_approx_error(self):
        should_be_deregistered = [
            "Customer", "Order", "Item", "Shipment"
        ]
        # Sample Dict to generate OCEL source
        events_other_workflow = [
            {"activity": "Register Guest", "objects": {"Guest": ["g1"]}},
            {"activity": "Create Booking", "objects": {"Guest": ["g1"], "Booking": ["b1"]}},
            {"activity": "Reserve Room", "objects": {"Booking": ["b1"]}},
            {"activity": "Check In", "objects": {"Guest": ["g1"], "Booking": ["b1"]}},
            {"activity": "Check Out", "objects": {"Guest": ["g1"], "Booking": ["b1"]}}
        ]

        ocel_source = dict_test_ocel_source([(self.events,5),(events_other_workflow,20)], shuffle=False)
        emitted_models_and_msg = []
        ocel_source.pipe(
            oc_operator(object_emit_threshold=0.15), # Because of the high object_emit_threshold, DEREGISTRATION cmd should be emitted
        ).subscribe(
            on_next=lambda x: emitted_models_and_msg.append(x),
        )

        emitted_commands = []
        for msg in emitted_models_and_msg:
            if msg["type"] == "model":
                self.assertIsInstance(msg["model"], HeuristicsNet)
            elif msg["type"] == "command":
                self.assertIsInstance(msg["command"], Command)
                emitted_commands.append(msg)
            elif msg["type"] == "aer_diagram":
                self.assertIsInstance(msg["model"], ActivityERDiagram)

        for msg in emitted_commands:
            if msg["command"] == Command.DEREGISTER:
                self.assertIn(msg["object_type"], should_be_deregistered)

    def test_oc_operator_aer_diagram_can_forget_relations(self):
        events_with_multiple_items= [
            {"activity": "Register Customer", "objects": {"Customer": ["c1"]}},
            {"activity": "Create Order", "objects": {"Customer": ["c1"], "Order": ["o1"]}},
            {"activity": "Add Item", "objects": {"Order": ["o1"], "Item": ["i1, i2"]}},
        ]
        events_with_single_item_but_multiple_orders = [
            {"activity": "Register Customer", "objects": {"Customer": ["c1"]}},
            {"activity": "Create Order", "objects": {"Customer": ["c1"], "Order": ["o1","o2"]}},
            {"activity": "Add Item", "objects": {"Order": ["o1","o2"], "Item": ["i1","i2"]}},
        ]

        ocel_source = dict_test_ocel_source([(events_with_multiple_items, 10), (events_with_single_item_but_multiple_orders,10)], shuffle=False)
        emitted_aer_models = []
        ocel_source.pipe(
            oc_operator(relation_max_approx_error=0.5, relation_model_update_frequency=30),  # High max_approx_error to trigger forgetting
            ops.filter(lambda output: output["type"] == "aer_diagram" and isinstance(output["model"], ActivityERDiagram)),
        ).subscribe(
            on_next=lambda x: emitted_aer_models.append(x),
        )
        self.assertEqual(Cardinality.ONE_TO_ONE, emitted_aer_models[0]["model"].get_relations("Add Item")[("Item","Order")])
        self.assertEqual(Cardinality.MANY_TO_MANY, emitted_aer_models[1]["model"].get_relations("Add Item")[("Item","Order")])
        self.assertEqual(Cardinality.ONE_TO_ONE, emitted_aer_models[0]["model"].get_relations("Create Order")[("Customer","Order")])
        self.assertEqual(Cardinality.ONE_TO_MANY, emitted_aer_models[1]["model"].get_relations("Create Order")[("Customer","Order")])

