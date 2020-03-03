import contextlib
import os
from pathlib import Path
from typing import Union


@contextlib.contextmanager
def working_directory(p: Union[Path, str]):
    """Changes working directory and returns to previous on exit."""
    prev_cwd = Path.cwd()
    os.chdir(p)
    try:
        yield
    finally:
        os.chdir(prev_cwd)
