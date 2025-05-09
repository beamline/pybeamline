import os
import unittest
from reactivex import Observable
from reactivex import operators as ops

from pybeamline.boevent import BOEvent
from pybeamline.sources.ocel_log_source import ocel_log_source_from_file


class TestOcelJsonLogSource(unittest.TestCase):
    def test_generate_ocel_source_from_file_with_wrong_path(self):
        test_file_path = "non_existent_file.jsonocel"
        with self.assertRaises(Exception) as context:
            ocel_log_source_from_file(test_file_path)
        self.assertIn("File does not exist", str(context.exception))

    def test_generate_ocel_source_from_file(self):
        # Path to the test file
        test_file_path = "tests/logistics.jsonocel"
        # Generate OCEL from the test file
        ocel_source = ocel_log_source_from_file(test_file_path)
        # Check if the generated OCEL is not None
        self.assertIsInstance(ocel_source, Observable)
        # Capture first 10 events
        emitted_events = []
        ocel_source.pipe(
            ops.map(lambda event: event),
            ops.take(10),
        ).subscribe(
            on_next=lambda x: emitted_events.append(x),
        )
        # Check if the number of emitted events matches the expected count
        self.assertEqual(len(emitted_events), 10)
        # Check if the emitted events are of type BOEvent
        for event in emitted_events:
            self.assertIsInstance(event, BOEvent)

