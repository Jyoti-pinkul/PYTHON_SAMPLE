def transform_nested_json(data):
    result = {}

    # Step 1: Identify the nested key that contains a list of dicts (like "C")
    nested_key = None
    for key in data:
        value = data[key]
        if isinstance(value, list) and len(value) > 0 and isinstance(value[0], dict):
            nested_key = key
            break

    if nested_key is None:
        return data  # No nested dictionary list found

    nested_items = data[nested_key]
    nested_dict = nested_items[0]  # Assume only one dict inside the list

    # Step 2: Find the key inside nested dict which is a list (assumed "account"-like field)
    account_key = None
    for key in nested_dict:
        if isinstance(nested_dict[key], list):
            account_key = key
            break

    if account_key is None:
        return data  # No expandable list inside the nested dict

    account_list = nested_dict[account_key]
    account_len = len(account_list)

    # Step 3: Collect other array fields from top-level data that match account length
    external_array_fields = {}
    for key in data:
        if key != nested_key:
            value = data[key]
            if isinstance(value, list) and len(value) == account_len:
                external_array_fields[key] = value
            else:
                result[key] = value  # Non-list or unmatched-length keys

    # Step 4: Expand each item in the account list with fixed values and external arrays
    expanded_list = []
    for i in range(account_len):
        item = {}
        for key in nested_dict:
            value = nested_dict[key]
            if isinstance(value, list):
                item[key] = value[i]
            else:
                item[key] = value

        for ext_key in external_array_fields:
            item[ext_key] = external_array_fields[ext_key][i]

        expanded_list.append(item)

    result[nested_key] = expanded_list
    return result



# test_json_transformer.py

import pytest
from json_transformer import transform_nested_json

def test_basic_transform():
    data = {
        "A": 123,
        "B": 234,
        "C": [{"account": [1, 2, 3], "aa": 1, "cc": 3}],
        "D": ["a", 2, 3]
    }
    expected = {
        "A": 123,
        "B": 234,
        "C": [
            {"account": 1, "aa": 1, "cc": 3, "D": "a"},
            {"account": 2, "aa": 1, "cc": 3, "D": 2},
            {"account": 3, "aa": 1, "cc": 3, "D": 3}
        ]
    }
    assert transform_nested_json(data) == expected

def test_missing_account_list():
    data = {
        "C": [{"aa": 1, "cc": 3}],
        "D": ["a", "b"]
    }
    assert transform_nested_json(data) == data  # no account-like list

def test_no_nested_key():
    data = {
        "A": 123,
        "B": 456
    }
    assert transform_nested_json(data) == data  # no nested key to transform

def test_multiple_array_fields():
    data = {
        "meta": [{"ids": [9, 8], "flag": True}],
        "x": ["alpha", "beta"],
        "y": [100, 200],
        "z": "keep"
    }
    expected = {
        "z": "keep",
        "meta": [
            {"ids": 9, "flag": True, "x": "alpha", "y": 100},
            {"ids": 8, "flag": True, "x": "beta", "y": 200}
        ]
    }
    assert transform_nested_json(data) == expected