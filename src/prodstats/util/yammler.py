from __future__ import annotations

import logging
import os
import tempfile
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Union

import yaml

logger = logging.getLogger(__name__)


class Yammler(dict):
    _no_dump = ["changed"]
    _metavars = ["fspath", "updated_at"]
    _data_key = "data"
    _meta_key = "meta"

    def __init__(self, fspath: Union[str, Path], data: dict = None):
        self.fspath = str(fspath)
        self.changed = False
        self.updated_at = self.stamp()
        data = {}
        if os.path.exists(fspath):
            with open(fspath) as f:
                data = yaml.safe_load(f) or {}
        super().__init__(data)

    def __enter__(self):
        """ Opens a context with the Yammler's file loaded and immediately dumps back to
            the backing yaml file when the context is exited."""
        return self

    def __exit__(self, *exc):
        if not exc[0]:
            self.dump()

    def dump(self) -> None:
        with self.durable(self.fspath, "w") as f:
            logger.debug(f"(Yammler) dumping to {self.fspath}")
            yaml.safe_dump(dict(self), f, default_flow_style=False)

    @staticmethod
    def stamp():
        return datetime.utcnow()

    # @classmethod
    # @contextmanager
    # def context(cls, fspath: Union[str, Path]):
    #     """Opens a context with the specified file loaded and immediately dumps back to a
    #         yaml file when the context is exited."""
    #     obj = cls(fspath)
    #     try:
    #         yield obj
    #     finally:
    #         obj.dump()

    @classmethod
    @contextmanager
    def durable(cls, fspath: str, mode: str = "w+b"):
        """ Safely dump the Yammler's context to a file by writing to an intermediate temporary
            file, then renaming the temporary file to overwrite the target file on disk
            (since file renaming operations are atomic)"""
        _fspath = fspath
        _mode = mode
        _file = tempfile.NamedTemporaryFile(_mode, delete=False)

        try:
            yield _file
        except Exception as e:
            # delete the temp file and propagate execption
            os.unlink(_file.name)
            raise e
        else:
            # close file and rename temp file to use target file name
            _file.close()
            os.rename(_file.name, _fspath)
