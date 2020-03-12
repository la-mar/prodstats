from typing import Optional

from pydantic import BaseSettings, SecretStr

__all__ = ["BasicAuth", "ClientCredentials"]


class HTTPAuth(BaseSettings):
    """ Base class for reading """

    def dict(self, reveal: bool = False, **kwargs):
        """ extend dict() to include option to reveal secret values """
        if reveal:
            return {
                k: v.get_secret_value() if isinstance(v, SecretStr) else v
                for k, v in super().dict(**kwargs).items()
            }
        else:
            return super().dict(**kwargs)


class BasicAuth(HTTPAuth):
    """ Read """

    username: Optional[SecretStr] = None
    password: Optional[SecretStr] = None


class ClientCredentials(HTTPAuth):
    client_id: Optional[SecretStr] = None
    client_secret: Optional[SecretStr] = None


class ClientAppAuth(BasicAuth):
    class Config:
        env_prefix = "client_app_"

    persisted: bool = False
