import logging

import pytest  # noqa

import util
from util.humanize import short_number, size_bytes
from util.iterables import apply_transformation, chunks

logger = logging.getLogger(__name__)


@pytest.fixture
def nested_dict():
    yield {
        "key": "value",
        "dict_key": {
            "nested_key": "10",
            "nested_dict_key": {"nested_key": "nested_value"},
            "nested_list_key": [
                "list_value",
                "700",
                ["nested_list_value", "nested_list_value2"],
            ],
        },
        "list_key": ["list_value", "700", ["nested_list_value", "nested_list_value2"]],
    }


@pytest.fixture
def tmpyaml(tmpdir):
    path = tmpdir.mkdir("test").join("yaml.yaml")
    path.write(
        """container:
            example:
                enabled: true
                model: db.models.TestModel
                ignore_unkown: true
            """
    )
    yield path


class TestGenericUtils:
    def test_chunks(self):
        values = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        expected = [[1, 2], [3, 4], [5, 6], [7, 8], [9, 10]]
        result = [list(x) for x in chunks(values, n=2)]
        assert result == expected

    def test_chunks_nested_iterables(self):
        values = [1, 2, 3, 4, [1, 2, 3, 4], [[1, 2, 3, 4], [1, 2, 3, 4], [1, 2, 3, 4]]]
        expected = [
            [1, 2],
            [3, 4],
            [[1, 2, 3, 4], [[1, 2, 3, 4], [1, 2, 3, 4], [1, 2, 3, 4]]],
        ]
        result = [list(x) for x in chunks(values, n=2)]
        assert result == expected

    @pytest.mark.parametrize(
        "data,expected", [(1, [1]), ([1, 2, 3], [1, 2, 3]), ((1), [(1)]), ({}, [{}])]
    )
    def test_ensure_list(self, data, expected):
        assert util.ensure_list(data) == expected

    @pytest.mark.parametrize(
        "data,expected",
        [
            ([1, 2, 3], [1, 2, 3]),
            ([[[1]]], 1),
            ([[1, 2, 3]], [1, 2, 3]),
            ([{1: 2}], {1: 2}),
            ({1: 2}, {1: 2}),
            (1, 1),
            ([[[1], 2]], [[1], 2]),
        ],
    )
    def test_reduce(self, data, expected):
        assert util.reduce(data) == expected


class TestHumanize:
    def test_size_bytes_zero_bytes(self):
        assert size_bytes(0) == "0B"

    def test_size_bytes_string_arg(self):
        assert size_bytes("123") == "123.0 B"

    @pytest.mark.parametrize(
        "number,expected",
        [(123456, "120.56 KB"), (1200000, "1.14 MB"), (1200000000, "1.12 GB")],
    )
    def test_size_bytes_format(self, number, expected):
        assert size_bytes(number) == expected

    @pytest.mark.parametrize(
        "number,round_digits,expected",
        [
            (1000, 0, "1K"),
            (100000, 0, "100K"),
            (1000000, 0, "1M"),
            (1400, 1, "1.4K"),
            (100400, 1, "100.4K"),
            (1400000, 1, "1.4M"),
        ],
    )
    def test_short_number(self, number, round_digits, expected):
        assert short_number(number, round_digits) == expected


class TestApplyTransformation:
    def test_nested_values(self, nested_dict):
        expected = {
            "key": "VALUE",
            "dict_key": {
                "nested_key": "10",
                "nested_dict_key": {"nested_key": "NESTED_VALUE"},
                "nested_list_key": [
                    "LIST_VALUE",
                    "700",
                    ["NESTED_LIST_VALUE", "NESTED_LIST_VALUE2"],
                ],
            },
            "list_key": [
                "LIST_VALUE",
                "700",
                ["NESTED_LIST_VALUE", "NESTED_LIST_VALUE2"],
            ],
        }

        result = apply_transformation(nested_dict, lambda x: str(x).upper())
        assert str(result) == str(expected)

    def test_nested_keys(self, nested_dict):
        expected = {
            "KEY": "value",
            "DICT_KEY": {
                "NESTED_KEY": "10",
                "NESTED_DICT_KEY": {"NESTED_KEY": "nested_value"},
                "NESTED_LIST_KEY": [
                    "list_value",
                    "700",
                    ["nested_list_value", "nested_list_value2"],
                ],
            },
            "LIST_KEY": [
                "list_value",
                "700",
                ["nested_list_value", "nested_list_value2"],
            ],
        }
        result = apply_transformation(
            nested_dict, lambda x: str(x).upper(), keys=True, values=False
        )

        assert str(result) == str(expected)

    def test_nested_keys_and_values(self, nested_dict):
        expected = {
            "KEY": "VALUE",
            "DICT_KEY": {
                "NESTED_KEY": "10",
                "NESTED_DICT_KEY": {"NESTED_KEY": "NESTED_VALUE"},
                "NESTED_LIST_KEY": [
                    "LIST_VALUE",
                    "700",
                    ["NESTED_LIST_VALUE", "NESTED_LIST_VALUE2"],
                ],
            },
            "LIST_KEY": [
                "LIST_VALUE",
                "700",
                ["NESTED_LIST_VALUE", "NESTED_LIST_VALUE2"],
            ],
        }
        result = apply_transformation(
            nested_dict, lambda x: str(x).upper(), keys=True, values=True
        )
        assert str(result) == str(expected)

    def test_handle_custom_type(self):
        class CustomType:
            pass

        data = {"key": "value", "obj_key": CustomType()}

        result = apply_transformation(
            data, lambda x: str(x).upper(), keys=True, values=True
        )

        assert result["KEY"] == "VALUE"
        assert isinstance(result["OBJ_KEY"], CustomType)
