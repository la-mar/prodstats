import pydoc
from types import ModuleType
from typing import Any, Dict, Union, no_type_check


def locate(obj_name: str, context: Union[ModuleType, Dict, str] = None) -> Any:
    resource = None  # type: ignore

    # try to import dotted resource name. ex: db.models.MyModel
    resource = pydoc.locate(obj_name)

    if not resource and context is not None:
        if isinstance(context, str):
            # assume context is a dotted module path and try to look it up
            resource = try_locate_in_module(obj_name, pydoc.locate(context))
        elif isinstance(context, dict):
            # assume context is the raw context mapping
            resource = try_locate_in_context(obj_name, context)

    if not resource and context is not None:
        # try the current module's globals as a last ditch resort
        resource = try_locate_in_context(obj_name, globals())

    if not resource:
        raise ValueError(f"Unable to locate resource: {obj_name}")

    return resource


@no_type_check
def try_locate_in_module(obj_name: str, context: Union[Dict, ModuleType, None]) -> Any:
    if isinstance(context, ModuleType):
        return getattr(context, obj_name)


@no_type_check
def try_locate_in_context(obj_name: str, context: Union[Dict, ModuleType, None]) -> Any:
    try:
        return context[obj_name]
    except:  # noqa
        return None
