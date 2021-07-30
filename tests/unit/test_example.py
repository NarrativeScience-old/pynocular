"""Contains tests for the example module"""

import unittest

from ns_sql_utils.example import foo


class ExampleTests(unittest.TestCase):
    """Tests showing an example"""

    def test_example(self):
        """Should be true"""
        self.assertEqual(foo(), "bar")
