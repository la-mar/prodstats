from __future__ import annotations

import enum
from typing import Any, Dict, List


class EnumMeta(type):
    def __iter__(self):
        return self.values()  # pragma: no cover


class Enum(enum.Enum):
    """Extends Enum builtin for easier lookups and iteration """

    __metaclass__ = EnumMeta

    @classmethod
    def value_map(cls) -> Dict[Any, Enum]:
        return cls._value2member_map_  # type: ignore

    @classmethod
    def has_member(cls, value: str) -> bool:
        """ Check if the enum has a member name matching the uppercased passed value """
        return hasattr(cls, str(value).upper())

    @classmethod
    def has_value(cls, value: str) -> bool:
        """ Check if the enum has a member name matching the passed value"""
        return value in cls.values()

    @classmethod
    def values(cls) -> List[Any]:
        return [v.value for v in cls.value_map().values()]

    @classmethod
    def keys(cls) -> List[str]:
        """Get a list of the enumerated attribute names """
        return [v.name for v in cls.value_map().values()]

    @classmethod
    def members(cls) -> List[Enum]:
        """ Get a list of instances containing all enumerated attributes """
        return list(cls.value_map().values())
