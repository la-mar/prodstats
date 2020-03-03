import re
from typing import Any, Callable, Union


class Criterion:
    """ Basic component of validation logic used to compose a parsing rule """

    def __init__(self, func: Callable, name: str = None):
        self.name = name or ""
        self.func = func

    def __call__(self, value: Any):
        return bool(self.func(value))

    def __repr__(self):
        return f"Criterion: name={self.name}, validator={self.func.__name__}"


class RegexCriterion(Criterion):
    """ Regex extraction harness for parser rule"""

    def __init__(self, regex: str, name: str = None):
        self.pattern = regex
        self.regex = re.compile(regex)
        super().__init__(func=self.regex.match, name=name)

    def __call__(self, value: Any):
        return super().__call__(str(value))

    def __repr__(self):
        return f"RegexCriterion: {self.name} - {self.pattern}"


class TypeCriterion(Criterion):
    """ Type check harness for parser rule """

    def __init__(self, dtype: type, name: str = None):
        func = lambda v: isinstance(v, dtype)  # noqa
        super().__init__(func=func, name=name)


class ValueCriterion(Criterion):
    """ Value comparison harness for parser rule """

    def __init__(self, value: Union[str, int, float, bool], name: str = None):
        func = lambda v: v == value  # noqa
        super().__init__(func=func, name=name)
