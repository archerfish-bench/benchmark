import unittest

from ..utils.result_set_match import compare_results


class TestCompareResults(unittest.TestCase):

    def test_compare_different_length(self):
        """
        Different length, rows are also different.
        """
        expected = [{'a': 1}]
        actual = [{'a': 1}, {'a': 2}]
        rules = []
        self.assertEqual((False, "Number of rows are different, example: exp={'a': 1}, act={'a': 1}, #exp=1, #act=2"),
                         compare_results(expected, actual, rules))

    def test_compare_different_length_distinct_rows(self):
        """
            Different length, rows are same.
            Should pass (consider it as distinct)
        """
        expected = [{'a': 1}]
        actual = [{'a': 1}, {'a': 1}]
        rules = []
        self.assertEqual((True, ''), compare_results(expected, actual, rules))

    def test_compare_different_length_with_dups(self):
        expected = [{}]
        actual = [{}, {}]
        rules = []
        self.assertTrue((True, ''), compare_results(expected, actual, rules))

    def test_compare_exact_match(self):
        expected = [{'a': 1, 'b': 'test'}]
        actual = [{'a': 1, 'b': 'test'}]
        rules = [{'match': 'exact', 'columns': ['a', 'b']}]
        self.assertEqual(compare_results(expected, actual, rules), (True, ""))

    def test_compare_exact_match_with_duplicates(self):
        """
            Different length, rows are same.
            Should pass (consider it as distinct)
        """
        expected = [{'a': 1, 'b': 'test'}]
        actual = [{'a': 1, 'b': 'test'}, {'a': 1, 'b': 'test'}, {'a': 1, 'b': 'test'}, {'a': 1, 'b': 'test'}]
        rules = [{'match': 'exact', 'columns': ['a', 'b']}]
        self.assertEqual(compare_results(expected, actual, rules), (True, ""))

    def test_compare_exact_no_match(self):
        expected = [{'a': 1, 'b': 'test'}]
        actual = [{'a': 2, 'b': 'test'}]
        rules = [{'match': 'exact', 'columns': ['a', 'b']}]
        self.assertEqual((False,
                          "Comparison failed for row: exp={'a': 1, 'b': 'test'}, act={'a': 2, 'b': "
                          "'test'}"),
                         compare_results(expected, actual, rules),
                         )

    def test_compare_oneof_match(self):
        expected = [{'a': 1}]
        actual = [{'a': 1, 'b': 2}]
        rules = [{'match': 'oneof', 'columns': ['a', 'b']}]
        self.assertEqual(compare_results(expected, actual, rules), (True, ""))

    def test_compare_oneof_no_match(self):
        expected = [{'a': 3, 'c': 4}]
        actual = [{'a': 1, 'b': 2}]
        rules = [{'match': 'oneof', 'columns': ['a', 'c']}]
        self.assertEqual((False, "Comparison failed for row: exp={'a': 3, 'c': 4}, act={'a': 1, 'b': 2}"),
                         compare_results(expected, actual, rules))

    def test_compare_float_tolerance(self):
        expected = [{'a': 1.000}]
        actual = [{'a': 1.0001}]
        rules = [{'match': 'exact', 'columns': ['a']}]
        self.assertEqual(compare_results(expected, actual, rules), (True, ""))

    def test_compare_none_values_match(self):
        expected = [{'a': None}]
        actual = [{'a': None}]
        rules = [{'match': 'exact', 'columns': ['a']}]
        self.assertEqual(compare_results(expected, actual, rules), (True, ""))

    def test_compare_none_values_no_match(self):
        expected = [{'a': None}]
        actual = [{'a': 1}]
        rules = [{'match': 'exact', 'columns': ['a']}]
        self.assertEqual((False, "Comparison failed for row: exp={'a': None}, act={'a': 1}"),
                         compare_results(expected, actual, rules))

    def test_compare_multiple_rules(self):
        expected = [{'a': 1, 'b': 'test', 'c': 2}]
        actual = [{'a': 1, 'b': 'test', 'c': 2}]
        rules = [{'match': 'exact', 'columns': ['a']}, {'match': 'oneof', 'columns': ['b', 'c']}]
        self.assertEqual(compare_results(expected, actual, rules), (True, ""))

    def test_compare_multiple_rules_match_one_of(self):
        expected = [{'a': 1, 'b': 'test', 'c': 3}]
        actual = [{'a': 1, 'b': 'test', 'c': 2}]
        rules = [{'match': 'exact', 'columns': ['a']}, {'match': 'oneof', 'columns': ['b', 'c']}]
        self.assertEqual(compare_results(expected, actual, rules), (True, ""))

    def test_compare_different_key_names_match_exact(self):
        expected = [{'a': 1}]
        actual = [{'b': 1}]
        rules = [{'match': 'exact', 'columns': ['a']}]
        self.assertEqual(compare_results(expected, actual, rules), (True, ""))

    def test_compare_different_key_names_match_oneof(self):
        expected = [{'a': 1}]
        actual = [{'b': 1, 'c': 2}]
        rules = [{'match': 'oneof', 'columns': ['a', 'b']}]
        self.assertEqual(compare_results(expected, actual, rules), (True, ""))

    def test_compare_multiple_columns_exact_match(self):
        expected = [{'a': 1, 'b': 'test'}]
        actual = [{'a': 1, 'b': 'test'}]
        rules = [{'match': 'exact', 'columns': ['a', 'b']}]
        self.assertEqual(compare_results(expected, actual, rules), (True, ""))

    def test_compare_multiple_columns_exact_no_match(self):
        expected = [{'a': 1, 'b': 'test'}]
        actual = [{'a': 1, 'b': 'testing'}]
        rules = [{'match': 'exact', 'columns': ['a', 'b']}]
        self.assertEqual((False,
                          "Comparison failed for row: exp={'a': 1, 'b': 'test'}, act={'a': 1, 'b': "
                          "'testing'}"), compare_results(expected, actual, rules))

    def test_compare_same_column_names_within_rows_match_1(self):
        expected = [{'a': 1, 'b': 'test'}, {'a': None, 'b': 'hello'}]
        actual = [{'a': 1, 'b': 'test'}, {'a': None, 'b': 'hello'}]
        rules = [{'match': 'exact', 'columns': ['a', 'b']}]
        self.assertEqual(compare_results(expected, actual, rules), (True, ""))

    def test_compare_same_column_names_within_rows_match_2(self):
        expected = [{'a': 1, 'b': 'test', 'c': 1}, {'a': None, 'b': 'hello', 'c': 2}]
        actual = [{'x': 1, 'y': 'test', 'z': 1}, {'x': None, 'y': 'hello', 'z': 2}]
        rules = [{'match': 'exact', 'columns': ['a', 'b']}]
        self.assertEqual(compare_results(expected, actual, rules), (True, ""))

        expected = [{'a': 1, 'b': 'test', 'c': 1}, {'a': None, 'b': 'hello', 'c': 2}]
        actual = [{'a': 1, 'b': 'test', 'c': 1}, {'a': None, 'b': 'hello', 'c': 2}]
        self.assertEqual(compare_results(expected, actual, rules), (True, ""))

    def test_dedup_compare_same_column_names_within_rows_match_2(self):
        expected = [{'a': 1, 'b': 'test', 'c': 1}, {'a': 1, 'b': 'test', 'c': 1}]
        actual = [{'x': 1, 'y': 'test', 'z': 1}, {'x': 1, 'y': 'test', 'z': 1}]
        rules = [{'match': 'exact', 'columns': ['a', 'b']}]
        self.assertEqual(compare_results(expected, actual, rules), (True, ""))

        expected = [{'a': 1, 'b': 'test', 'c': 1}, {'a': 1, 'b': 'test', 'c': 1}]
        actual = [{'a': 1, 'b': 'test', 'c': 1}, {'a': 1, 'b': 'test', 'c': 1}]
        self.assertEqual(compare_results(expected, actual, rules), (True, ""))

    def test_compare_same_column_names_within_rows_no_match(self):
        expected = [{'a': 1, 'b': 'test'}, {'a': None, 'b': 'hello'}]
        actual = [{'x': 1, 'y': 'test'}, {'x': 2, 'y': 'hello'}]
        rules = [{'match': 'exact', 'columns': ['a', 'b']}]
        self.assertEqual((False,
                          "Comparison failed for row: exp={'a': None, 'b': 'hello'}, act={'x': 2, 'y': "
                          "'hello'}"), compare_results(expected, actual, rules))


if __name__ == '__main__':
    unittest.main()
