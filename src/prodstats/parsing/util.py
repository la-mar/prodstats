import re

__all__ = ["camel_to_snake"]


REGEX_CAMEL_CASE_HANDLE_ACRONYMS = re.compile(r"(.)([A-Z][a-z]+)")
REGEX_CAMEL_CASE = re.compile(r"([a-z0-9])([A-Z])")


def camel_to_snake(name):
    name = REGEX_CAMEL_CASE_HANDLE_ACRONYMS.sub(r"\1_\2", name)
    return REGEX_CAMEL_CASE.sub(r"\1_\2", name).lower()
