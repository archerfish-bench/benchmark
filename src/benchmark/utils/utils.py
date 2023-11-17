def include_filter(data: list, filter_values: list):
    """
    Filter data based on one or more query_info values.

    Args:
        data (list): data entries to be filtered.
        filter_values (list): A list of filter values to filter the data by.

    Returns:
        list: A filtered list of data entries.
    """
    new_data = {}
    for value in data:
        if value['query_name'] in filter_values:
            new_data[value['query_name']] = value
    return new_data


def exclude_filter(data: list, filter_values: list):
    """
    Filter data based on one or more query_info values.

    Args:
        data (list): data entries to be filtered that are in main dataset.
        filter_values (list): A list of query_info values to exclude

    Returns:
        list: A filtered list of data entries.
    """
    new_data = {}
    for value in data:
        if value['query_name'] not in filter_values:
            new_data[value['query_name']] = value
    return new_data

def filter_schema(data: list, schema: str):
    """
    Filter data based on one or more query_info values.

    Args:
        data (list): data entries to be filtered.
        schema (str): Schema that should be included

    Returns:
        list: A filtered list of data entries.
    """
    new_data = {}
    for value in data:
        if schema in value['schemas']:
            new_data[value['query_name']] = value
    return new_data