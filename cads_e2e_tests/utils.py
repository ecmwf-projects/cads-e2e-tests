import contextlib
import hashlib
import logging
import os
import tempfile
import time
import traceback
from typing import Any, Iterator, Type

import typer

from . import exceptions


@contextlib.contextmanager
def catch_exception(
    report: dict[str, Any],
    logger: logging.Logger,
    elapsed_time: bool,
    allowed_exceptions: tuple[Type[Exception], ...],
) -> Iterator[float]:
    tic = time.perf_counter()
    try:
        yield tic
    except allowed_exceptions:
        logger.exception("")
        report["tracebacks"].append(traceback.format_exc())
    else:
        toc = time.perf_counter()
        if elapsed_time:
            report["elapsed_time"] = toc - tic


@contextlib.contextmanager
def tmp_working_dir() -> Iterator[str]:
    old_dir = os.getcwd()
    with tempfile.TemporaryDirectory() as tmpdir:
        os.chdir(tmpdir)
        try:
            yield tmpdir
        finally:
            os.chdir(old_dir)


def check_report(
    report: dict[str, Any],
    ext: str | None = None,
    size: int | None = None,
    time: float | None = None,
    checksum: str | None = None,
) -> dict[str, Any]:
    if ext is not None:
        _, actual_ext = os.path.splitext(report["target"])
        if actual_ext != ext:
            raise exceptions.ExtensionError(f"{actual_ext!r} != {ext!r}")

    if size is not None:
        actual_size = report["size"]
        if actual_size != size:
            raise exceptions.SizeError(f"{actual_size!r} != {size!r}")

    if time is not None:
        actual_time = report["elapsed_time"]
        if actual_time > time:
            raise exceptions.TimeError(f"{actual_time!r} > {time!r}")

    if checksum is not None:
        actual_checksum = report["checksum"]
        if actual_checksum != checksum:
            raise exceptions.ChecksumError(f"{actual_checksum!r} != {checksum!r}")

    return report


def validate_request(request: dict[str, Any]) -> None:
    assert isinstance(request, dict)
    assert "collection_id" in request
    assert set(request) <= {"collection_id", "parameters", "checks"}

    assert isinstance(request["collection_id"], str)

    parameters = request.get("parameters", {})
    assert isinstance(parameters, dict)

    checks = request.get("checks", {})
    assert isinstance(checks, dict)
    assert set(checks) <= {"size", "ext", "time", "checksum"}


def get_target_info(target: str) -> dict[str, Any]:
    info: dict[str, Any] = {}
    info["target"] = target

    info["size"] = os.path.getsize(target)

    with open(target, "rb") as f:
        digest = hashlib.file_digest(f, "md5")
    info["checksum"] = digest.hexdigest()

    return info


def print_passed_vs_failed(report: list[dict[str, Any]]) -> None:
    failed = sum(True for request in report if request["tracebacks"])
    passed = len(report) - failed
    failed_perc = failed * 100 / len(report)
    passed_perc = passed * 100 / len(report)
    if failed:
        typer.secho(f"FAILED: {failed} ({failed_perc:.1f}%)", fg=typer.colors.RED)
    if passed:
        typer.secho(f"PASSED: {passed} ({passed_perc:.1f}%)", fg=typer.colors.GREEN)
