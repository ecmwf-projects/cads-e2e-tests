import contextlib
import dataclasses
import functools
import hashlib
import logging
import os
import tempfile
import traceback
from typing import Iterator, Type

from cads_api_client import Results


@contextlib.contextmanager
def tmp_working_dir() -> Iterator[str]:
    old_dir = os.getcwd()
    with tempfile.TemporaryDirectory() as tmpdir:
        os.chdir(tmpdir)
        try:
            yield tmpdir
        finally:
            os.chdir(old_dir)


@contextlib.contextmanager
def catch_exceptions(
    tracebacks: list[str],
    exceptions: tuple[Type[Exception]] = (Exception,),
    logger: logging.Logger | None = None,
) -> Iterator[None]:
    try:
        yield
    except exceptions:
        if logger:
            logger.exception("")
        tracebacks.append(traceback.format_exc())


@dataclasses.dataclass
class Target:
    results: Results

    @functools.cached_property
    def target(self) -> str:
        return self.results.download()

    @property
    def checksum(self) -> str:
        with open(self.target, "rb") as f:
            digest = hashlib.file_digest(f, "md5")
        return digest.hexdigest()

    @property
    def extension(self) -> str:
        _, extension = os.path.splitext(self.target)
        return extension

    @property
    def size(self) -> int:
        return os.path.getsize(self.target)

    @property
    def content_length(self) -> int:
        return self.results.content_length

    @property
    def content_type(self) -> str:
        return self.results.content_type
