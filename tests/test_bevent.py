from datetime import datetime
from unittest import TestCase

from pybeamline.bevent import BEvent


class TestBEvent(TestCase):
    def test_get_process_name(self):
        e1 = BEvent("act-a", "case-id")
        self.assertEqual(e1.get_process_name(), "ProcessName")
        e2 = BEvent("act-a", "case-id", process_name="process-name")
        self.assertEqual(e2.get_process_name(), "process-name")

    def test_get_trace_name(self):
        e = BEvent("act-a", "case-id")
        self.assertEqual(e.get_trace_name(), "case-id")

    def test_get_event_name(self):
        e = BEvent("act-a", "case-id")
        self.assertEqual(e.get_event_name(), "act-a")

    def test_get_event_time(self):
        t = datetime.now()
        e = BEvent("act-a", "case-id", event_time=t)
        self.assertEqual(e.get_event_time(), t)
