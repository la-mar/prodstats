import pytest

import parsing.util as putil


class TestCamelCaseToSnakeCase:
    def test_idempotency(self):
        expected = "already_snake_case"
        assert putil.camel_to_snake(expected) == expected

    @pytest.mark.parametrize(
        "data,expected",
        [
            ("getStatusCode", "get_status_code"),
            ("getHTTPStatusCode", "get_http_status_code"),
            ("getStatusCodeHTTP", "get_status_code_http"),
            ("getHTTPStatusCodeHTTP", "get_http_status_code_http"),
        ],
    )
    def test_handle_first_word_not_capitalized(self, data, expected):
        assert putil.camel_to_snake(data) == expected

    @pytest.mark.parametrize(
        "data,expected",
        [
            ("HTTPResponseCodeSSL", "http_response_code_ssl"),
            ("getHTTPStatusCode", "get_http_status_code"),
            ("getStatusCodeHTTP", "get_status_code_http"),
            ("HTTPStatusCode", "http_status_code"),
        ],
    )
    def test_handle_embedded_acronym(self, data, expected):
        assert putil.camel_to_snake(data) == expected
