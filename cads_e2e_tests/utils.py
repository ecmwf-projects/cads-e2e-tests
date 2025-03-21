import contextlib
import dataclasses
import datetime
import hashlib
import logging
import os
import random
import tempfile
import traceback
from typing import Any, Iterator, Type


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
class TargetInfo:
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


def random_date(start: str, end: str) -> str:
    start_date = datetime.date.fromisoformat(start)
    end_date = datetime.date.fromisoformat(end)
    days = random.randint(0, (end_date - start_date).days)
    random_date = start_date + datetime.timedelta(days=days)
    return random_date.isoformat()


def reorder(
    requests: list[Any],
    cyclic: bool,
    randomise: bool,
    n_repeats: int,
) -> list[Any]:
    if cyclic:
        output = []
        for _ in range(n_repeats):
            output.extend(
                random.sample(requests, len(requests)) if randomise else requests
            )
        return output

    return [
        request
        for request in (
            random.sample(requests, len(requests)) if randomise else requests
        )
        for _ in range(n_repeats)
    ]
