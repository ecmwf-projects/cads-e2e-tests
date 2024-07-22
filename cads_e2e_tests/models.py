import json
import logging
from typing import Any, BinaryIO, ContextManager, TextIO

import pydantic_core
from pydantic import BaseModel

from . import exceptions, utils

LOGGER = logging.getLogger(__name__)


class Checks(BaseModel):
    checksum: str | None = None
    extension: str | None = None
    size: int | None = None
    time: float | None = None

    def check_checksum(self, actual: str) -> None:
        expected = self.checksum
        if expected is not None and actual != expected:
            raise exceptions.ChecksumError(actual=actual, expected=expected)

    def check_extension(self, actual: str) -> None:
        expected = self.extension
        if expected is not None and actual != expected:
            raise exceptions.ExtensionError(actual=actual, expected=expected)

    def check_size(self, actual: int) -> None:
        expected = self.size
        if expected is not None and actual != expected:
            raise exceptions.SizeError(actual=actual, expected=expected)

    def check_time(self, actual: float) -> None:
        expected = self.time
        if expected is not None and actual > expected:
            raise exceptions.TimeError(actual=actual, expected=expected)


class Request(BaseModel):
    collection_id: str
    parameters: dict[str, Any] = {}
    checks: Checks = Checks()


class Report(BaseModel):
    request: Request
    tracebacks: list[str] = []
    request_uid: str | None = None
    checksum: str | None = None
    extension: str | None = None
    size: int | None = None
    time: float | None = None

    def catch_exceptions(self) -> ContextManager[None]:
        return utils.catch_exceptions(
            self.tracebacks, exceptions=(exceptions.CheckError,), logger=LOGGER
        )

    def run_checks(self) -> None:
        if self.checksum is not None:
            with self.catch_exceptions():
                self.request.checks.check_checksum(self.checksum)

        if self.extension is not None:
            with self.catch_exceptions():
                self.request.checks.check_extension(self.extension)

        if self.size is not None:
            with self.catch_exceptions():
                self.request.checks.check_size(self.size)

        if self.time is not None:
            with self.catch_exceptions():
                self.request.checks.check_time(self.time)


def load_reports(fp: TextIO | BinaryIO) -> list[Report]:
    return [Report(**report) for report in json.load(fp)]


def dump_reports(reports: list[Report], fp: TextIO | BinaryIO) -> None:
    return json.dump(pydantic_core.to_jsonable_python(reports), fp)
