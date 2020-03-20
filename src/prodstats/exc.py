from typing import Any, List, Tuple


def split_errors(objs: List[Any]) -> Tuple[List[Any], List[BaseException]]:
    errors: List[Any] = []
    for idx, x in enumerate(objs):
        if isinstance(x, BaseException):
            errors.append(objs.pop(idx))

    return objs, errors


# class RootException(Exception):
#     pass


# class AuthError(RootException):
#     pass
