import unittest

from pybeamline.algorithms.oc_operator import OCOperator


class TestOCOperator():
    def setUp(self):
        # Initialize the OCOperator with a mock control flow
        self.operator_with_cf = OCOperator(control_flow={
            "Order": lambda x: x,
            "Payment": lambda x: x,
        })
        self.operator_without_cf = OCOperator()

    def test_oc_operator(self):
       # Check if the operator is initialized correctly with control flow
        self.assertIsInstance(self.operator_with_cf, OCOperator)
        self.assertFalse(self.operator_with_cf.dynamic_mode)

        # Check if the operator is initialized correctly without control flow
        self.assertIsInstance(self.operator_without_cf, OCOperator)
        self.assertTrue(self.operator_without_cf.dynamic_mode)


