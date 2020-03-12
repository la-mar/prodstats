import pytest

import util.iterables as it


@pytest.fixture
def queryable():
    yield {
        "slim": {
            "enabled": True,
            "exclude": [],
            "model": "ProdStat",
            "path": "/path/to/endpoint",
            "aliases": {"key": "value", "key2": "value2"},
            "tasks": [
                {
                    "name": "example_sync",
                    # "mode": "sync",
                    "cron": {"minute": "*/15"},
                    "options": None,
                },
                {
                    "name": "example_full",
                    # "mode": "full",
                    "cron": {"minute": 0, "hour": 0, "day_of_week": 0},
                    "options": None,
                },
            ],
        }
    }


def test_make_hash_from_list():
    items = ["eat", "more", "creatine", 1, 2, 3]
    hash1 = it.make_hash(items)
    assert hash1 == it.make_hash(items)


def test_make_hash_from_simple_mapping():
    items = {"key": "value", "another_key": "another_value"}
    hash1 = it.make_hash(items)
    assert hash1 == it.make_hash(items)


def test_make_hash_from_complex_mapping(queryable):
    hash1 = it.make_hash(queryable)
    assert hash1 == it.make_hash(queryable)


def test_query_nested_collection(queryable):
    expected = "example_full"
    assert it.query("slim.tasks.1.name", data=queryable) == expected


def test_filter_by_prefix_defaults():
    items = {
        "mykey_special": "special_value",
        "mykey_extra_special": "extra_special_value",
        "unwanted": "unwanted",
        "mykey_not_special": "not_special_value",
    }

    expected = {
        "special": "special_value",
        "extra_special": "extra_special_value",
        "not_special": "not_special_value",
    }
    assert it.filter_by_prefix(items, "mykey") == expected


def test_filter_by_prefix_no_strip():
    items = {
        "mykey_special": "special_value",
        "mykey_extra_special": "extra_special_value",
        "unwanted": "unwanted",
        "mykey_not_special": "not_special_value",
    }

    expected = {
        "mykey_special": "special_value",
        "mykey_extra_special": "extra_special_value",
        "mykey_not_special": "not_special_value",
    }
    assert it.filter_by_prefix(items, "mykey", strip=False) == expected


def test_filter_by_prefix_to_lower():
    items = {
        "Mykey_special": "special_value",
        "mykey_ExTra_special": "extra_special_value",
        "unwanted": "unwanted",
        "mykey_NOT_special": "not_special_value",
    }

    expected = {
        "special": "special_value",
        "extra_special": "extra_special_value",
        "not_special": "not_special_value",
    }
    assert it.filter_by_prefix(items, "mykey", tolower=True) == expected


def test_filter_by_prefix_to_upper():
    items = {
        "Mykey_special": "special_value",
        "mykey_ExTra_special": "extra_special_value",
        "unwanted": "unwanted",
        "mykey_NOT_special": "not_special_value",
    }

    expected = {
        "SPECIAL": "special_value",
        "EXTRA_SPECIAL": "extra_special_value",
        "NOT_SPECIAL": "not_special_value",
    }
    assert it.filter_by_prefix(items, "mykey", tolower=False) == expected


@pytest.mark.parametrize("sep", [".", "/", ":"])
def test_query_factory(sep):
    data = dict(a=1, b=[dict(d=5, e=dict(f=6, g=8)), dict(h=4)], c="c")
    get = it.query_factory(data, sep=sep)
    assert get(f"b{sep}0{sep}e{sep}f") == 6
    assert get(f"b{sep}0{sep}e") == {"f": 6, "g": 8}


def test_distinct_by_key():
    data = [
        {"name": "a", "attribute": 1},
        {"name": "a", "attribute": 2},
        {"name": "b", "attribute": 3},
        {"name": "c", "attribute": 4},
        {"name": "b", "attribute": 5},
        {None: "x", "attribute": -1},
        {"": "x", "attribute": -1},
    ]

    expected = [
        {"name": "a", "attribute": 2},
        {"name": "b", "attribute": 5},
        {"name": "c", "attribute": 4},
    ]
    actual = it.distinct_by_key(data, key="name")

    assert actual == expected
