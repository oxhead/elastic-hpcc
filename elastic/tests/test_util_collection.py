import unittest
import copy

from elastic.util import collection as util_collection


class TestCollection(unittest.TestCase):

    def setUp(self):
        self.x1 = {
            'a':
                {'b': 1}
        }

    def tearDown(self):
        pass

    def test_recursive_update_api(self):
        x2 = util_collection.recursive_update(copy.deepcopy(self.x1), 'a.b', 1)
        self.assertEqual(self.x1, x2)

    def test_recursive_update_not_change(self):
        x2 = util_collection.recursive_update(copy.deepcopy(self.x1), 'a.b'.split('.'), 1)
        self.assertEqual(self.x1, x2)

    def test_recursive_update_new_key(self):
        x2 = util_collection.recursive_update(copy.deepcopy(self.x1), 'a.c'.split('.'), 2)
        self.assertNotEqual(self.x1, x2)

    def test_recursive_update_new_key2(self):
        x2 = util_collection.recursive_update({}, 'a.b'.split('.'), 1)
        self.assertEqual(self.x1, x2)

    def test_recursive_lookup_exists(self):
        self.assertEqual(util_collection.recursive_lookup(self.x1, "a.b"), 1)

    def test_recursive_lookup_not_exist1(self):
        self.assertRaises(Exception, util_collection.recursive_lookup, self.x1, "a.c")

    def test_recursive_lookup_not_exist2(self):
        self.assertEqual(util_collection.recursive_lookup(self.x1, "a.c", True), True)


if __name__ == "__main__":
    unittest.main()
