import unittest

# TODO: Check if this is fine.
from ..utils.utils import include_filter
from ..utils.utils import exclude_filter


class TestFilterDataByQueryInfo(unittest.TestCase):

    def setUp(self):
        self.data = [
            {'query_name': 'info1', 'other_field': 'value1'},
            {'query_name': 'info2', 'other_field': 'value2'},
            {'query_name': 'info3', 'other_field': 'value3'}
        ]

    def test_include_filter_single_value(self):
        result = include_filter(self.data, ['info1'])
        self.assertEqual(len(result), 1)
        self.assertEqual(result.get('info1')['query_name'], 'info1')

    def test_include_filter_multiple_vals(self):
        result = include_filter(self.data, ['info1', 'info3'])
        self.assertEqual(len(result), 2)
        self.assertEqual(result.get('info1')['query_name'], 'info1')
        self.assertEqual(result.get('info3')['query_name'], 'info3')

    def test_include_invalid_filter(self):
        result = include_filter(self.data, ['info4'])
        self.assertEqual(len(result), 0)

    def test_exclude_filter_single_val(self):
        result = exclude_filter(self.data, ['info1'])
        self.assertEqual(len(result), 2)
        self.assertEqual(result.get('info2')['query_name'], 'info2')
        self.assertEqual(result.get('info3')['query_name'], 'info3')

    def test_exclude_multiple_vals(self):
        result = exclude_filter(self.data, ['info1', 'info3'])
        self.assertEqual(len(result), 1)
        self.assertEqual(result.get('info2')['query_name'], 'info2')

    def test_exclude_invalid_val(self):
        result = exclude_filter(self.data, ['info4'])
        self.assertEqual(len(result), 3)


if __name__ == "__main__":
    unittest.main()
