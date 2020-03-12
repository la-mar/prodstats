from schemas.credentials import BasicAuth, ClientCredentials, HTTPAuth


class TestHTTPAuth:
    def test_init_empty(self):
        assert HTTPAuth().dict() == {}


class TestBasicAuth:
    def test_no_data(self):
        expected = {"username": None, "password": None}
        assert BasicAuth().dict() == expected
        assert BasicAuth().dict(reveal=True) == expected

    def test_reveal_secrets(self):
        data = {"username": "name", "password": "pass"}
        assert (
            str(BasicAuth(**data).dict())
            == "{'username': SecretStr('**********'), 'password': SecretStr('**********')}"
        )
        assert (
            str(BasicAuth(**data).dict(reveal=True))
            == "{'username': 'name', 'password': 'pass'}"
        )


class TestClientCredentials:
    def test_no_data(self):
        expected = {"client_id": None, "client_secret": None}
        assert ClientCredentials().dict() == expected
        assert ClientCredentials().dict(reveal=True) == expected

    def test_reveal_secrets(self):
        data = {"client_id": "name", "client_secret": "pass"}
        assert (
            str(ClientCredentials(**data).dict())
            == "{'client_id': SecretStr('**********'), 'client_secret': SecretStr('**********')}"
        )
        assert (
            str(ClientCredentials(**data).dict(reveal=True))
            == "{'client_id': 'name', 'client_secret': 'pass'}"
        )
