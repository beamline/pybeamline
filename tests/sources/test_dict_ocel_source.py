import unittest

from pybeamline.algorithms.lambda_operator import LambdaOperator
from pybeamline.boevent import BOEvent
from pybeamline.sources.dict_ocel_test_source import dict_test_ocel_source

class TestDictOcelSource(unittest.TestCase):
    def setUp(self):
        # Sample Dict to generate OCEL source
        self.dict = [
        {"activity": "Register Customer", "objects": {"Customer": ["c1"]}},
        {"activity": "Create Order", "objects": {"Customer": ["c1"], "Order": ["o1"]}},
        {"activity": "Add Item", "objects": {"Order": ["o1"], "Item": ["i1"]}},
        {"activity": "Add Item", "objects": {"Order": ["o1"], "Item": ["i2"]}},
        {"activity": "Reserve Item", "objects": {"Item": ["i1"]}},
        {"activity": "Pack Item", "objects": {"Item": ["i1","i2"], "Order": ["o1"]}},
        {"activity": "Ship Item", "objects": {"Item": ["i1","i2"], "Shipment": ["s1"]}},
        {"activity": "Send Invoice", "objects": {"Order": ["o1"], "Invoice": ["inv1"]}},
        {"activity": "Receive Review", "objects": {"Customer": ["c1"], "Order": ["o1"]}},
        ]
        self.dict2 = [
            {"activity": "Register Customer", "objects": {"Customer": ["c2"]}},
            {"activity": "Create Order", "objects": {"Customer": ["c2"], "Order": ["o2"]}},
            {"activity": "Add Item", "objects": {"Order": ["o2"], "Item": ["i2"]}},
            {"activity": "Reserve Item", "objects": {"Item": ["i2"]}},
            {"activity": "Cancel Order", "objects": {"Customer": ["c2"], "Order": ["o2"]}}
        ]


    def test_generate_shuffled_traces(self):
        # Test the generate_shuffled_traces function
        shuffled_traces = dict_test_ocel_source([(self.dict2,5),(self.dict,10)], shuffle=True)

        # Capture the emitted events
        emitted_events = []
        shuffled_traces.pipe(
            LambdaOperator(lambda event: event)
        ).subscribe(
            on_next=lambda x: emitted_events.append(x),
        )
        # Check if the number of emitted events matches the expected count
        self.assertEqual(len(emitted_events), 115) # 10 + 5 = 15 events from dict and dict2, multiplied by 10 and 5 respectively
        # Check if the emitted events are of type BOEvent
        for i, event in enumerate(emitted_events):
            self.assertIsInstance(event, BOEvent)
            self.assertEqual(event.get_event_id(), f"e{i}",)
            self.assertEqual(event.get_vmap(), {})
        self.assertIsNotNone(emitted_events[0].to_dict())
        self.assertEqual(emitted_events[0].__repr__(), str(emitted_events[0]))

        # Check that the events are shuffled
        # This is a simple check, as we cannot guarantee the order of the events
        # We can check that the events from dict and dict2 are present
        dict_events = [event for event in emitted_events if event.get_event_name() in ["Register Customer", "Create Order", "Add Item", "Reserve Item", "Pack Item", "Ship Item", "Send Invoice", "Receive Review"]]
        dict2_events = [event for event in emitted_events if event.get_event_name() in ["Register Customer", "Create Order", "Add Item", "Reserve Item", "Cancel Order"]]
        self.assertGreater(len(dict_events), 0)
        self.assertGreater(len(dict2_events), 0)

