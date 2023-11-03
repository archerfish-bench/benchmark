def include_filter(data: list, query_info_values: list):
    """
    Filter data based on one or more query_info values.

    Args:
        data (list): data entries to be filtered.
        query_info_values (list): A list of query_info values to filter the data by.

    Returns:
        list: A filtered list of data entries.
    """
    new_data = {}
    for value in data:
        if value['query_name'] in query_info_values:
            new_data[value['query_name']] = value
    return new_data


def exclude_filter(data: list, query_info_values: list):
    """
    Filter data based on one or more query_info values.

    Args:
        data (list): data entries to be filtered that are in main dataset.
        query_info_values (list): A list of query_info values to exclude

    Returns:
        list: A filtered list of data entries.
    """
    new_data = {}
    for value in data:
        if value['query_name'] not in query_info_values:
            new_data[value['query_name']] = value
    return new_data
