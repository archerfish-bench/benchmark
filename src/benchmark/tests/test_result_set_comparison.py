import unittest
from decimal import Decimal

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

    def test_compare_exact_match_case_sensitive_key(self):
        expected = [{'a': 1, 'b': 'test'}]
        actual = [{'a': 1, 'B': 'test'}]
        rules = [{'match': 'exact', 'columns': ['a', 'b']}]
        self.assertEqual(compare_results(expected, actual, rules), (True, ""))

    def test_compare_with_quotes(self):
        expected = [{'a': 1, 'b': '"test"'}]
        actual = [{'a': 1, 'B': 'test'}]
        rules = [{'match': 'exact', 'columns': ['a', 'b']}]
        self.assertEqual(compare_results(expected, actual, rules), (True, ""))

        expected = [{'a': 1, 'b': '"test'}]
        actual = [{'a': 1, 'B': 'test'}]
        rules = [{'match': 'exact', 'columns': ['a', 'b']}]
        self.assertEqual(compare_results(expected, actual, rules), (True, ""))

        expected = [{'a': 1, 'b': 'test"'}]
        actual = [{'a': 1, 'B': 'test'}]
        rules = [{'match': 'exact', 'columns': ['a', 'b']}]
        self.assertEqual(compare_results(expected, actual, rules), (True, ""))

        expected = [{'a': 1, 'b': '""'}]
        actual = [{'a': 1, 'B': ''}]
        rules = [{'match': 'exact', 'columns': ['a', 'b']}]
        self.assertEqual(compare_results(expected, actual, rules), (True, ""))

        expected = [{'a': 1, 'b': ' '}]
        actual = [{'a': 1, 'B': ''}]
        rules = [{'match': 'exact', 'columns': ['a', 'b']}]
        self.assertNotEquals(compare_results(expected, actual, rules), (True, ""))

    def test_mix_of_exact_oneof_a(self):
        expected = [
            {"COUNTRY_NAME": "USA", "COUNTRY_CODE": "US", "population": 331002651,
             "official_language_percentage": 80.2},
            {"COUNTRY_NAME": "CANADA", "COUNTRY_CODE": "CA", "population": 37742154,
             "official_language_percentage": 75.2}
        ]

        actual = [
            {"C1": "USA1", "C2": "US", "C3": 331002651, "C4": 80.199},
            {"C1": "CANADA1", "C2": "CA", "C3": 37742154, "C4": 75.199}
        ]

        rules = [
            {"columns": ["official_language_percentage", "population"], "match": "exact"},
            {"columns": ["COUNTRY_NAME", "COUNTRY_CODE"], "match": "oneof"}
        ]

        self.assertEqual(compare_results(expected, actual, rules), (True, ""))

        rules = [
            {"columns": ["official_language_percentage", "population"], "match": "exact"},
            {"columns": ["COUNTRY_NAME", "COUNTRY_CODE"], "match": "exact"}
        ]

        self.assertNotEquals(compare_results(expected, actual, rules), (True, ""))

    def test_mix_of_exact_oneof_b(self):
        expected = [
            {"COUNTRY_NAME": "USA", "COUNTRY_CODE": "US", "population": 331002651,
             "official_language_percentage": 80.2},
            {"COUNTRY_NAME": "CANADA", "COUNTRY_CODE": "CA", "population": 37742154,
             "official_language_percentage": 75.2}
        ]

        actual = [
            {"C1": "USA", "C2": "Different_Code", "C3": 331002651, "C4": 80.199},
            {"C1": "CANADA", "C2": "Different_Code", "C3": 37742154, "C4": 75.199}
        ]

        rules = [
            {"columns": ["COUNTRY_NAME", "COUNTRY_CODE"], "match": "oneof"}
        ]

        self.assertEqual(compare_results(expected, actual, rules), (True, ""))

    def test_mix_of_exact_oneof_c(self):
        expected = [
            {"COUNTRY_NAME": "USA", "COUNTRY_CODE": "US", "population": 331002651,
             "official_language_percentage": 80.2},
            {"COUNTRY_NAME": "CANADA", "COUNTRY_CODE": "CA", "population": 37742154,
             "official_language_percentage": 75.2}
        ]

        actual = [
            {"C1": "Different_Country", "C2": "Different_Code", "C3": 331002651, "C4": 80.199},
            {"C1": "Different_Country", "C2": "Different_Code", "C3": 37742154, "C4": 75.199}
        ]

        # one of should fail
        rules = [
            {"columns": ["COUNTRY_NAME", "COUNTRY_CODE"], "match": "oneof"}
        ]

        self.assertNotEquals(compare_results(expected, actual, rules), (True, ""))


    def test_mix_of_exact_d(self):
        expected = [
            {"COUNTRY_NAME": "USA", "COUNTRY_CODE": "US", "population": 331002651,
             "official_language_percentage": 80.1},
            {"COUNTRY_NAME": "CANADA", "COUNTRY_CODE": "CA", "population": 37742154,
             "official_language_percentage": 75.1}
        ]

        actual = [
            {"C1": "USA", "C2": "US", "C3": 331002651, "C4": 80.1},
            {"C1": "CANADA", "C2": "CA", "C3": 37742154, "C4": 75.1}
        ]

        rules = [
            {"columns": ["official_language_percentage", "population"], "match": "exact"}
        ]

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

    def test_compare_with_duplicates(self):
        """
            Single column with duplicates
            Should pass (consider it as distinct)
        """
        expected = [{'a': 1}]
        actual = [{'a': 1}, {'a': 1}]
        rules = [{'match': 'exact', 'columns': ['a']}]
        self.assertEqual(compare_results(expected, actual, rules), (True, ""))

        rules = [{'match': 'exact', 'columns': ['*']}]
        self.assertEqual(compare_results(expected, actual, rules), (True, ""))

        actual = [{'a': 1}, {'a': 1}, {'a': 2}]
        rules = [{'match': 'exact', 'columns': ['a']}]
        self.assertNotEquals(compare_results(expected, actual, rules), (True, ""))

        rules = [{'match': 'exact', 'columns': ['*']}]
        self.assertNotEquals(compare_results(expected, actual, rules), (True, ""))

    def test_with_real_data(self):
        actual = [{'employee_id': 2, 'start_from': '2003', 'shop_id': 1, 'is_full_time': 'T'},
                  {'employee_id': 7, 'start_from': '2008', 'shop_id': 6, 'is_full_time': 'T'},
                  {'employee_id': 1, 'start_from': '2009', 'shop_id': 1, 'is_full_time': 'T'},
                  {'employee_id': 6, 'start_from': '2010', 'shop_id': 2, 'is_full_time': 'F'},
                  {'employee_id': 3, 'start_from': '2011', 'shop_id': 8, 'is_full_time': 'F'},
                  {'employee_id': 4, 'start_from': '2012', 'shop_id': 4, 'is_full_time': 'T'},
                  {'employee_id': 5, 'start_from': '2013', 'shop_id': 5, 'is_full_time': 'T'}]
        expected = [{'shop_id': 1, 'employee_id': 1, 'start_from': '2009', 'is_full_time': 'T'},
                    {'shop_id': 1, 'employee_id': 2, 'start_from': '2003', 'is_full_time': 'T'},
                    {'shop_id': 8, 'employee_id': 3, 'start_from': '2011', 'is_full_time': 'F'},
                    {'shop_id': 4, 'employee_id': 4, 'start_from': '2012', 'is_full_time': 'T'},
                    {'shop_id': 5, 'employee_id': 5, 'start_from': '2013', 'is_full_time': 'T'},
                    {'shop_id': 2, 'employee_id': 6, 'start_from': '2010', 'is_full_time': 'F'},
                    {'shop_id': 6, 'employee_id': 7, 'start_from': '2008', 'is_full_time': 'T'}]
        rules = [{'columns': ['*'], 'match': 'exact'}]
        self.assertEqual((True, ""), compare_results(expected, actual, rules))

        rules = [{'columns': ['shop_id'], 'match': 'exact'}]
        self.assertEqual((True, ""), compare_results(expected, actual, rules))

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

        expected = [{'a': 1.000}]
        actual = [{'a': 1.1001}]
        rules = [{'match': 'exact', 'columns': ['a']}]
        self.assertNotEqual(compare_results(expected, actual, rules), (True, ""))

        expected = [{'a': Decimal(1.000)}]
        actual = [{'a': Decimal(1.1001)}]
        rules = [{'match': 'exact', 'columns': ['a']}]
        self.assertNotEqual(compare_results(expected, actual, rules), (True, ""))

        expected = [{'a': Decimal(1.000)}]
        actual = [{'a': Decimal(1.1001)}]
        rules = [{'match': 'exact', 'columns': ['a']}]
        self.assertNotEqual(compare_results(expected, actual, rules), (True, ""))

        # Restricts to 1 decimal place, so it will be true. Intensional
        expected = [{'a': 0.00051}]
        actual = [{'a': 0.00053}]
        rules = [{'match': 'exact', 'columns': ['a']}]
        self.assertEqual(compare_results(expected, actual, rules), (True, ""))

        # Restricts to 1 decimal place, so it will be true. Intensional
        expected = [{'a': Decimal(0.00051)}]
        actual = [{'a': Decimal(0.00053)}]
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

    def test_compare_string_with_int(self):
        expected = [{'a': 0, 'b': '1', 'c': 2.5}, {'a': 0.0, 'b': '1.5', 'c': 3.67}]
        actual = [{'a': '0', 'b': 1, 'c': '2.5'}, {'a': '0.0', 'b': 1.5, 'c': '3.67'}]
        rules = [{'match': 'exact', 'columns': ['a', 'b', 'c']}]
        self.assertEqual(compare_results(expected, actual, rules), (True, ""))

    def test_compare_null_with_0(self):
        expected = [{'a': 0, 'b': '1', 'c': None}, {'a': 0.0, 'b': '1.5', 'c': None}]
        actual = [{'a': '0', 'b': 1, 'c': 0}, {'a': '0.0', 'b': 1.5, 'c': 0}]
        rules = [{'match': 'exact', 'columns': ['a', 'b', 'c']}]
        self.assertEqual(compare_results(expected, actual, rules), (True, ""))
        self.assertEqual(compare_results(actual, expected, rules), (True, ""))

        expected = [{'a': 0, 'b': '1', 'c': 0}, {'a': 0.0, 'b': '1.5', 'c': 0}]
        actual = [{'a': '0', 'b': 1, 'c': None}, {'a': '0.0', 'b': 1.5, 'c': 0}]
        rules = [{'match': 'exact', 'columns': ['a', 'b', 'c']}]
        self.assertEqual(compare_results(expected, actual, rules), (True, ''))


if __name__ == '__main__':
    unittest.main()
