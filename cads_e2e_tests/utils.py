import contextlib
import dataclasses
import hashlib
import logging
import os
import tempfile
import traceback
from typing import Iterator, Type


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
    target: str

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


@dataclasses.dataclass
class RemoteTarget:
    asset: dict[str, str]

    @property
    def checksum(self) -> str:
        return "Not implemented for remote results"

    @property
    def extension(self) -> str:
        _, extension = os.path.splitext(self.asset["href"])
        return extension

    @property
    def result_type(self) -> str:
        _, extension = os.path.splitext(self.asset["type"])
        return extension

    @property
    def size(self) -> int:
        return int(self.asset["file:size"])
