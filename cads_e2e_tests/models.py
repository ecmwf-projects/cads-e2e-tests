import datetime
import json
import logging
from typing import Any, BinaryIO, ContextManager, TextIO

import yaml
from pydantic import BaseModel, Field

from . import exceptions, utils

LOGGER = logging.getLogger(__name__)

if not hasattr(BaseModel, "model_dump"):  # pydantic 1
    setattr(BaseModel, "model_dump", BaseModel.dict)


class Checks(BaseModel):
    checksum: str | None = None
    extension: str | None = None
    size: int | None = None
    time: float | None = None
    content_length: int | None = None
    content_type: str | None = None

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

    def check_content_length(self, actual: int) -> None:
        expected = self.content_length
        if expected is not None and actual != expected:
            raise exceptions.ContentLengthError(actual=actual, expected=expected)

    def check_content_type(self, actual: str) -> None:
        expected = self.content_type
        if expected is not None and actual != expected:
            raise exceptions.ContentTypeError(actual=actual, expected=expected)


class Settings(BaseModel):
    max_runtime: float | None = None
    add_random_parameters: bool | None = None


class Request(BaseModel):
    collection_id: str
    parameters: dict[str, Any] = {}
    checks: Checks = Checks()
    settings: Settings = Settings()


class Report(BaseModel):
    request: Request
    started_at: datetime.datetime = Field(default_factory=datetime.datetime.now)
    finished_at: datetime.datetime = Field(default_factory=datetime.datetime.now)
    tracebacks: list[str] = []
    request_uid: str | None = None
    checksum: str | None = None
    content_length: int | None = None
    content_type: str | None = None
    extension: str | None = None
    size: int | None = None
    time: float | None = None

    def catch_exceptions(self, tracebacks: list[str]) -> ContextManager[None]:
        return utils.catch_exceptions(
            tracebacks, exceptions=(exceptions.CheckError,), logger=LOGGER
        )

    def run_checks(self) -> list[str]:
        tracebacks = list(self.tracebacks)
        if self.checksum is not None:
            with self.catch_exceptions(tracebacks=tracebacks):
                self.request.checks.check_checksum(self.checksum)

        if self.content_length is not None:
            with self.catch_exceptions(tracebacks=tracebacks):
                self.request.checks.check_content_length(self.content_length)

        if self.content_type is not None:
            with self.catch_exceptions(tracebacks=tracebacks):
                self.request.checks.check_content_type(self.content_type)
        if self.extension is not None:
            with self.catch_exceptions(tracebacks=tracebacks):
                self.request.checks.check_extension(self.extension)

        if self.size is not None:
            with self.catch_exceptions(tracebacks=tracebacks):
                self.request.checks.check_size(self.size)

        if self.time is not None:
            with self.catch_exceptions(tracebacks=tracebacks):
                self.request.checks.check_time(self.time)

        return tracebacks


def load_reports(fp: TextIO | BinaryIO) -> list[Report]:
    return [Report(**json.loads(line.strip())) for line in fp]


def dump_report(report: Report, fp: TextIO) -> None:
    try:
        import pydantic_core
    except ImportError:
        from pydantic.json import pydantic_encoder

        json.dump(report, fp, default=pydantic_encoder)
    else:
        json.dump(pydantic_core.to_jsonable_python(report), fp)
    fp.write("\n")


def load_requests(fp: TextIO | BinaryIO) -> list[Request]:
    return [Request(**request) for request in yaml.safe_load(fp)]


def dump_requests(requests: list[Request], fp: TextIO | BinaryIO) -> None:
    yaml.safe_dump([request.model_dump() for request in requests], fp)
