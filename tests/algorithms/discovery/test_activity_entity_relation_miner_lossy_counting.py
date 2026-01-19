import unittest
from datetime import datetime, timedelta

from pybeamline.algorithms.discovery.activity_entity_relation_miner_lossy_counting import activity_entity_relations_miner_lossy_counting, ActivityEntityRelationMinerLossyCounting
from pybeamline.boevent import BOEvent
from pybeamline.stream.stream import Stream
from pybeamline.utils.cardinality import Cardinality
from reactivex.operators import to_list


class TestActivityEntityRelationMinerLossyCounting(unittest.TestCase):

    def test_unary_relation_tracking(self):
        miner = ActivityEntityRelationMinerLossyCounting(max_approx_error=0.1)
        ts = datetime.now()

        events = [
            BOEvent("e1", "Create", {"Order": {"o1"}}, ts),
            BOEvent("e2", "Create", {"Order": {"o2"}}, ts),
            BOEvent("e3", "Create", {"Order": {"o3"}}, ts)
        ]

        for e in events:
            miner.ingest_event(e)

        model = miner.get_model()
        self.assertEqual(model.get_object_types("Create"), {"Order"})

    def test_binary_relation_tracking(self):
        miner = ActivityEntityRelationMinerLossyCounting(max_approx_error=0.1)
        ts = datetime.now()

        events = [
            BOEvent("e1", "Add", {"Order": {"o1"}, "Item": {"i1", "i2"}}, ts),
            BOEvent("e2", "Add", {"Order": {"o2"}, "Item": {"i3", "i4"}}, ts),
            BOEvent("e3", "Add", {"Order": {"o3"}, "Item": {"i5"}}, ts),
        ]

        for e in events:
            miner.ingest_event(e)

        model = miner.get_model()
        relations = model.get_relations("Add")
        self.assertIn(("Item", "Order"), relations)
        self.assertEqual(relations[("Item", "Order")], Cardinality.MANY_TO_ONE)

    def test_cardinality_inference(self):
        miner = ActivityEntityRelationMinerLossyCounting(max_approx_error=0.1)
        ts = datetime.now()

        # Simulate 1..many: 1 Order, 2 Items
        event = BOEvent("e1", "Pack", {"Order": {"o1"}, "Item": {"i1", "i2"}}, ts)
        miner.ingest_event(event)

        # Simulate 1..1
        event = BOEvent("e2", "Pack", {"Order": {"o2"}, "Item": {"i3"}},  ts)
        miner.ingest_event(event)

        model_1 = miner.get_model()
        relations = model_1.get_relations("Pack")
        self.assertIn(("Item", "Order"), relations)
        self.assertEqual(relations[("Item", "Order")], Cardinality.MANY_TO_ONE)

        event = BOEvent("e2", "Pack", {"Order": {"o3"}, "Item": {"i4"}},  ts)
        miner.ingest_event(event)

        model_2 = miner.get_model()
        relations = model_2.get_relations("Pack")
        self.assertIn(("Item", "Order"), relations)
        self.assertEqual(relations[("Item", "Order")], Cardinality.ONE_TO_ONE)

    def test_lossy_cleanup(self):
        # Low error → small bucket width → frequent cleanup
        miner = ActivityEntityRelationMinerLossyCounting(max_approx_error=0.25)
        ts = datetime.now()

        events = [
            BOEvent(f"e{i}", "Ship", {"Customer": {f"c{i}"}, "Package": {f"p{i}"}}, ts + timedelta(seconds=i))
            for i in range(4)
        ]
        events2 = [
            BOEvent(f"e{i}", "Ship", {"Customer": {f"c{i}"}, "Package": {f"p{i}",f"p1{i+2}"},"Order": {f"o{i}",f"o1{i+2}"}}, ts + timedelta(seconds=i))
            for i in range(4,9)
        ]
        events.extend(events2)
        for e in events:
            miner.ingest_event(e)

        model = miner.get_model()
        relations = model.get_relations("Ship")
        self.assertIn(("Customer", "Package"), relations)
        self.assertIn(("Customer", "Order"), relations)
        self.assertIn(("Order", "Package"), relations)

        self.assertEqual(relations[("Customer", "Package")], Cardinality.ONE_TO_MANY)
        self.assertEqual(relations[("Customer", "Order")], Cardinality.ONE_TO_MANY)
        self.assertEqual(relations[("Order", "Package")], Cardinality.MANY_TO_MANY)

        self.assertIn("ActivityER:", model.__str__())
        self.assertIn("Ship: Customer → Package [ONE_TO_MANY]", model.__str__())

        expected = "ActivityER(activities=['Ship'],"
        self.assertIn(expected, model.__repr__())


    def test_operator_stream_output(self):
        events = [
            BOEvent("e1", "Reserve", {"Item": {"i1"}}, datetime.now()),
            BOEvent("e2", "Reserve", {"Item": {"i2"}}, datetime.now()),
            BOEvent("e3", "Reserve", {"Item": {"i3"}}, datetime.now()),
            BOEvent("e4", "Reserve", {"Item": {"i4"}}, datetime.now()),
            BOEvent("e5", "Reserve", {"Item": {"i5"}}, datetime.now()),
        ]

        miner = activity_entity_relations_miner_lossy_counting(model_update_frequency=5, max_approx_error=0.2)
        result = Stream.from_iterable(events).pipe(miner).to_list()

        self.assertEqual(len(result), 1)
        model = result[0]
        unary = model.get_object_types("Reserve")
        self.assertIn("Item", unary)
        self.assertIn("ActivityER", model.__str__())

    def test_unary_to_relation_conversion(self):
        miner = ActivityEntityRelationMinerLossyCounting(max_approx_error=0.025)
        ts = datetime.now()

        events = [
            BOEvent("e1", "Register", {"Customer": {"c1"}}, ts),
            BOEvent("e2", "Register", {"Customer": {"c2"}}, ts),
            BOEvent("e3", "Register", {"Customer": {"c3"}}, ts),
            BOEvent("e3", "Register", {"Customer": {"c3"}}, ts)
        ]
        events2 = [
            BOEvent("e1", "Register", {"Customer": {"c1"}, "Order": {"c1"}}, ts),
            BOEvent("e2", "Register", {"Customer": {"c2"}, "Order": {"c1"}}, ts),
            BOEvent("e3", "Register", {"Customer": {"c3"}, "Order": {"c1"}}, ts),
            BOEvent("e3", "Register", {"Customer": {"c3"}, "Order": {"c1"}}, ts)
        ]
        events.extend(events2)
        for e in events:
            miner.ingest_event(e)

        model = miner.get_model()
        relations = model.get_relations("Register")
        self.assertIn(("Customer","Order"), relations)
        self.assertEqual(relations[("Customer","Order")], Cardinality.ONE_TO_ONE)