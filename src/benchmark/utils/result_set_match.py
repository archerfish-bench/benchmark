"""
I have two json arrays (named expected, actual) from database needs comparison, both are arraies and format like

[ {"key1": val1, "key2", val2, ...} ]


These JSON arraries are from database, I want to do the comparison with the following rows:

1. If #rows are different, return False
2. row order doesn't matter.
4. column name (keys) is not matter, so don't compare with the column name.
5. column order doesn't matter.
6. Should compare string to string, number to number (could float to int).
7. For decimal, round off to 1 decimal place.

As part of the comparison, you will receive a rules object, looks like:

"comparison_rules": [
  {
    "columns": [
      "official_language_percentage", "population"
    ],
    "match": "exact"
  },
  {
    "columns": [
      "COUNTRY_NAME", "COUNTRY_CODE"
    ],
    "match": "oneof"
  }
]

Which means:
- Two column from actual result must match "official_language_percentage" and "population" of expected.
- One actual result column must match either "COUNTRY_NAME" or "COUNTRY_CODE" column of expected. (It is fine if all columns present in expected)
- column names are from expected result instead of actual result

function is defined as `def compare_results(expected, actual, comparison_rules)`

Output of the function: When comparison failed, return a error_message in addition to the True, False, which including:
- Why comparison failed.
- One row from each list.

Examples of the match
Assume comparison_rules = [
    {"columns": ["official_language_percentage", "population"], "match": "exact"},
    {"columns": ["COUNTRY_NAME", "COUNTRY_CODE"], "match": "oneof"}
]

a. Match both of the oneof columns (Return True)
--
expected = [
    {"COUNTRY_NAME": "USA", "COUNTRY_CODE": "US", "population": 331002651, "official_language_percentage": 80.2},
    {"COUNTRY_NAME": "CANADA", "COUNTRY_CODE": "CA", "population": 37742154, "official_language_percentage": 75.2}
]

actual = [
    {"C1": "USA", "C2": "US", "C3": 331002651, "C4": 80.199},
    {"C1": "CANADA", "C2": "CA", "C3": 37742154, "C4": 75.199}
]


b. Match one of the oneof columns: (Return True)
--
expected = [
    {"COUNTRY_NAME": "USA", "COUNTRY_CODE": "US", "population": 331002651, "official_language_percentage": 80.2},
    {"COUNTRY_NAME": "CANADA", "COUNTRY_CODE": "CA", "population": 37742154, "official_language_percentage": 75.2}
]

actual = [
    {"C1": "USA", "C2": "Different_Code", "C3": 331002651, "C4": 80.199},
    {"C1": "CANADA", "C2": "Different_Code", "C3": 37742154, "C4": 75.199}
]


c. Missing oneof: (Return False)
--
expected = [
    {"COUNTRY_NAME": "USA", "COUNTRY_CODE": "US", "population": 331002651, "official_language_percentage": 80.2},
    {"COUNTRY_NAME": "CANADA", "COUNTRY_CODE": "CA", "population": 37742154, "official_language_percentage": 75.2}
]

actual = [
    {"C1": "Different_Country", "C2": "Different_Code", "C3": 331002651, "C4": 80.199},
    {"C1": "Different_Country", "C2": "Different_Code", "C3": 37742154, "C4": 75.199}
]

d. Missing exact: (Return False)
--
expected = [
    {"COUNTRY_NAME": "USA", "COUNTRY_CODE": "US", "population": 331002651, "official_language_percentage": 80.2},
    {"COUNTRY_NAME": "CANADA", "COUNTRY_CODE": "CA", "population": 37742154, "official_language_percentage": 75.2}
]

actual = [
    {"C1": "USA", "C2": "US", "C3": 331002651, "C4": 80.1},
    {"C1": "CANADA", "C2": "CA", "C3": 37742154, "C4": 75.1}
]
"""
import decimal
import logging
from typing import Optional


def compare_results(expected, actual, comparison_rules, intent_based_match: bool = True,
                    source_db_type: Optional[str] = None,
                    target_db_type: Optional[str] = None):
    if not intent_based_match:
        return compare_exact_match(expected, actual)

    # Function to remove leading and trailing quotes
    def remove_quotes(string):
        if isinstance(string, str):
            return string.strip('"')
        return string

    # Convert all keys in expected and actual to lowercase for case-insensitive comparison
    expected = [{(k.lower() if k is not None else None): v for k, v in row.items()} for row in expected]
    actual = [{(k.lower() if k is not None else None): v for k, v in row.items()} for row in actual]

    # Remove quotes in string values in expected and actual
    expected = [{k: remove_quotes(v) for k, v in row.items()} for row in expected]
    actual = [{k: remove_quotes(v) for k, v in row.items()} for row in actual]

    # for each rules of comparison_rules, convert the columns to lower case
    if comparison_rules:
        for rule in comparison_rules:
            rule["columns"] = [column.lower() for column in rule["columns"]]

    def remove_duplicates(input_list, columns: list = None):
        """
        Remove duplicates from list(dict)
        Weird that dict in python is not hashable, so we need to convert it to tuple
        """
        seen = {}
        result = []
        for item in input_list:
            # Convert dict to a tuple of items
            # If columns are specified, use only those for creating tuple_representation
            if columns:
                tuple_representation = tuple((k, item[k]) for k in columns if k in item)
            else:
                # If no columns specified, use all columns
                tuple_representation = tuple(item.items())
            if tuple_representation not in seen:
                seen[tuple_representation] = True
                result.append(item)
        return result

    # Get unique elements. We don't bother about duplicates in final result
    # First get all columns from comparison_rules
    cols = []
    for rule in comparison_rules:
        cols.extend(rule['columns'])

    if len(expected) > 0 and len(actual) > 0:
        expected_cols = expected[0].keys()
        actual_cols = actual[0].keys()

        # Proceed with de-dup only when expected and actual cols are same
        if expected_cols == actual_cols:
            if cols != ['*'] or (len(expected_cols) == 1 and len(actual_cols) == 1):
                expected_cols_to_use = cols if cols != ['*'] else expected_cols
                actual_cols_to_use = cols if cols != ['*'] else actual_cols
                expected = remove_duplicates(expected, expected_cols_to_use)
                actual = remove_duplicates(actual, actual_cols_to_use)
        else:
            logging.info("Expected and actual columns are different, skipping de-duplication in results")


    if len(expected) != len(actual):
        return False, (f"Number of rows are different, example: exp={expected[0] if len(expected) > 0 else 'null'}, "
                       f"act={actual[0] if len(actual) > 0 else 'null'}, #exp={len(expected)}, #act={len(actual)}")

    def is_match(expected_val, actual_val):
        '''
        This function compares expected_val and actual_val with relevant datatype conversion
        '''
        if ((expected_val is None or expected_val == 0) and
                (actual_val is None or actual_val == 0)):
            return True
        elif (isinstance(expected_val, (int, float, decimal.Decimal))
              and isinstance(actual_val, (int, float, decimal.Decimal))):
            return actual_val is not None and (str(round(expected_val, 1)) == str(round(actual_val, 1)))
        elif isinstance(expected_val, str) and isinstance(actual_val, str):
            return str(actual_val) == expected_val
        elif isinstance(expected_val, str) and isinstance(actual_val, (int, float)):
            return expected_val == str(actual_val)
        elif isinstance(expected_val, (int, float)) and isinstance(actual_val, str):
            return str(expected_val) == actual_val
        elif isinstance(expected_val, (int, float)) and isinstance(actual_val, (int, float)):
            return (actual_val is not None) and (
                    round(expected_val, 0) == round(actual_val, 0))
        else:
            return str(expected_val) == str(actual_val)

    def match_rule(rule, expected_row, actual_row):
        if rule["match"] == "exact":
            if '*' in rule["columns"]:
                columns_to_compare = expected_row.keys()
            else:
                columns_to_compare = rule["columns"]
            for column in columns_to_compare:
                if column in actual_row:
                    if not is_match(expected_row[column], actual_row[column]):
                        return False
                else:
                    column_matched = any(
                        is_match(expected_row[column], actual_val) for actual_val in actual_row.values())
                    if not column_matched:
                        return False
            return True

        elif rule["match"] == "oneof":
            for column in rule["columns"]:
                for key in actual_row.keys():
                    expected_value = expected_row[column]
                    actual_value = actual_row[key]
                    if is_match(expected_value, actual_value):
                        del actual_row[key]
                        return True
            return False

    for expected_row in expected:
        matched = False
        for actual_row in actual:
            actual_row_copy = actual_row.copy()
            rule_matched = True
            for rule in comparison_rules:
                if not match_rule(rule, expected_row, actual_row_copy):
                    rule_matched = False
                    break
            if rule_matched:
                matched = True
                actual.remove(actual_row)
                break
        if not matched:
            return False, f"Comparison failed for row: exp={expected_row}, act={actual_row}"
    return True, ""


def compare_exact_match(expected, actual):
    """
    Compare the expected and actual results exactly.
    """
    if len(expected) != len(actual):
        return False, f"Number of rows are different, #exp={len(expected)}, #act={len(actual)}"

    for expected_row, actual_row in zip(expected, actual):
        if expected_row != actual_row:
            return False, f"Comparison failed for row: exp={expected_row}, act={actual_row}"

    return True, ""
