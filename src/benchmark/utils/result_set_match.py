"""
I have two json arrays (named expected, actual) from database needs comparison, both are arraies and format like

[ {"key1": val1, "key2", val2, ...} ]


These JSON arraries are from database, I want to do the comparison with the following rows:

1. If #rows are different, return False
2. row order doesn't matter.
4. column name (keys) is not matter, so don't compare with the column name.
5. column order doesn't matter.
6. Should compare string to string, number to number (could float to int).
7. when compare with two numbers, episilon should be 0.001

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

I have the following Python program:

```python
def compare_results(expected, actual, comparison_rules):
    if len(expected) != len(actual):
        return False, "Number of rows are different"

    def match_rule(rule, expected_row, actual_row):
        if rule["match"] == "exact":
            for column in rule["columns"]:
                if expected_row[column] is None:
                    if actual_row.get(column) is not None:
                        return False
                elif isinstance(expected_row[column], str):
                    if expected_row[column] != actual_row.get(column):
                        return False
                elif isinstance(expected_row[column], (int, float)):
                    if actual_row.get(column) is None or abs(expected_row[column] - actual_row.get(column, 0)) > 0.001:
                        return False
        elif rule["match"] == "oneof":
            for column in rule["columns"]:
                if expected_row[column] == actual_row.get(column):
                    break
            else:
                return False
        return True

    for expected_row in expected:
        for actual_row in actual:
            for rule in comparison_rules:
                if not match_rule(rule, expected_row, actual_row):
                    break
            else:
                actual.remove(actual_row)
                break
        else:
            return False, f"Comparison failed for row: {expected_row}"
    return True, ""

```

You need help to fix it, there're some issues:
- It assume column names are same for expected and actual, it can be very different, you need to handle that (copy actual row during match, removing the matched columns from copied row if it matches)

Output (only produce Python program):
"""
import json
import logging
from typing import Optional



def compare_results(expected, actual, comparison_rules,
                    source_db_type: Optional[str] = None, target_db_type: Optional[str] = None):
    # make a copy of expected
    expected = expected.copy()
    # make a copy of actual
    actual = actual.copy()
    # make a copy of comparison_rules
    if comparison_rules:
        comparison_rules = comparison_rules.copy()
    new_expected = []
    # for key in expected, convert the key to lower case
    for row in expected:
        # create a new dict
        new_row = {}
        for key in row:
            v = row[key]
            new_row[key.lower()] = v
        # replace the row with new_row
        new_expected.append(new_row)
    expected = new_expected

    new_actual = []
    # for key in actual, convert the key to lower case
    for row in actual:
        # create a new dict
        new_row = {}
        for key in row:
            v = row[key]
            new_row[key.lower()] = v
        # replace the row with new_row
        new_actual.append(new_row)
    actual = new_actual

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
            expected = remove_duplicates(expected, cols)
            actual = remove_duplicates(actual, cols)
        else:
            logging.info("Expected and actual columns are different, skipping de-duplication in results")

    expected = remove_duplicates(expected)
    actual = remove_duplicates(actual)

    if len(expected) != len(actual):
        if target_db_type is not None and target_db_type == "mongo" and len(expected) < len(actual):
            pass
        else:
            return False, f"Number of rows are different, example: exp={expected[0] if len(expected) > 0 else 'null'}, act={actual[0] if len(actual) > 0 else 'null'}, #exp={len(expected)}, #act={len(actual)}"

    def match_rule(rule, expected_row, actual_row):
        if rule["match"] == "exact":
            if '*' in rule["columns"]:
                columns_to_compare = expected_row.keys()
            else:
                columns_to_compare = rule["columns"]

            for column in columns_to_compare:
                matched = False
                expected_value = None
                if column in expected_row:
                    expected_value = expected_row[column]
                elif "*" in expected_row:
                    expected_value = expected_row["*"]
                else:
                    return False
                for key in actual_row.keys():
                    if expected_value is None:
                        if actual_row[key] is None:
                            matched = True
                            del actual_row[key]
                            break
                    elif isinstance(expected_value, str) and isinstance(actual_row[key], str):
                        if expected_value == actual_row[key]:
                            matched = True
                            del actual_row[key]
                            break
                    elif isinstance(expected_value, (int, float)) and isinstance(actual_row[key], (int, float)):
                        if actual_row[key] is not None and (
                                round(expected_value, 0) == round(actual_row[key], 0)):
                            matched = True
                            del actual_row[key]
                            break
                if not matched:
                    return False
            return True
        elif rule["match"] == "oneof":
            for column in rule["columns"]:
                expected_value = None
                if column in expected_row:
                    expected_value = expected_row[column]
                elif "*" in expected_row:
                    expected_value = expected_row["*"]
                for key in actual_row.keys():
                    if expected_value is None:
                        if actual_row[key] is None:
                            return True
                            del actual_row[key]
                            break
                    elif isinstance(expected_value, str) and isinstance(actual_row[key], str):
                        if expected_value == actual_row[key]:
                            del actual_row[key]
                            return True
                            break
                    elif isinstance(expected_value, (int, float)) and isinstance(actual_row[key], (int, float)):
                        if actual_row[key] is not None and (
                                round(expected_value, 0) == round(actual_row[key], 0)):
                            del actual_row[key]
                            return True
                            break
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
