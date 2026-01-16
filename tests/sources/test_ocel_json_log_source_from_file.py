from pathlib import Path
import unittest
from typing import Any

from pybeamline.boevent import BOEvent
from pybeamline.mappers.take_mapper import take
from pybeamline.sources.ocel2_log_source_from_file import ocel2_log_source_from_file
from pybeamline.stream.base_sink import BaseSink
from pybeamline.stream.stream import Stream


class TestOcelJsonLogSource(unittest.TestCase):
    def test_generate_ocel_source_from_file_with_wrong_path(self):
        test_file_path = "non_existent_file.jsonocel"
        with self.assertRaises(Exception) as context:
            ocel2_log_source_from_file(test_file_path)
        self.assertIn("File does not exist", str(context.exception))

    def test_generate_ocel_source_from_file(self):

        class CollectorSink(BaseSink[Any]):
            def __init__(self):
                self.elements = []

            def consume(self, item: Any) -> None:
                self.elements.append(item)

        # Path to the test file
        test_file_path = str(Path(__file__).parent.parent / "logistics.jsonocel")
        # Generate OCEL from the test file
        ocel_source = ocel2_log_source_from_file(test_file_path)
        # Check if the generated OCEL is not None
        self.assertIsInstance(ocel_source, Stream)
        # Capture first 10 events
        collector = CollectorSink()
        ocel_source.pipe(
            take(10),
        ).sink(collector)

        # Check if the number of emitted events matches the expected count
        self.assertEqual(len(collector.elements), 10)

        # Check if the emitted events are of type BOEvent
        for event in collector.elements:
            self.assertIsInstance(event, BOEvent)



