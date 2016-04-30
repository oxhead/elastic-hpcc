import unittest
import copy
from elastic import base

from elastic.util import collection as util_collection


class TestHPCCRoxie(unittest.TestCase):

    def setUp(self):
        self.node = base.Node("", "10.25.11.85")

    def tearDown(self):
        pass

    def test_get_metrics(self):
        from elastic.hpcc import roxie
        output = roxie.get_metrics(self.node)
        self.assertTrue(len(output) > 0)


if __name__ == "__main__":
    unittest.main()
