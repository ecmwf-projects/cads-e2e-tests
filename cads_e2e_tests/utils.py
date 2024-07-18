import contextlib
import logging
import os
import tempfile
import time
import traceback
from typing import Any, Iterator, Type

import typer


@contextlib.contextmanager
def catch_exception(
    report: dict[str, Any],
    logger: logging.Logger,
    elapsed_time: bool = False,
    exceptions: tuple[Type[Exception], ...] = (Exception,),
) -> Iterator[float]:
    tic = time.perf_counter()
    try:
        yield tic
    except exceptions:
        logger.exception("")
        report["tracebacks"].append(str(traceback.format_exc()))
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
) -> dict[str, Any]:
    if ext is not None:
        _, actual_ext = os.path.splitext(report["target"])
        assert actual_ext == ext, f"{actual_ext!r} != {ext!r}"

    if size is not None:
        actual_size = report["size"]
        assert actual_size == size, f"{actual_size!r} != {size!r}"

    if time is not None:
        actual_time = report["elapsed_time"]
        assert actual_time <= time, f"{actual_time!r} > {time!r}"

    return report


def validate_request(request: dict[str, Any]) -> None:
    assert isinstance(request, dict)
    assert "collection_id" in request
    assert set(request) <= {"collection_id", "parameters", "checks"}

    assert isinstance(request["collection_id"], str)

    parameters = request.get("parameters", {})
    assert isinstance(parameters, dict)

    checks = request.get("checks", {})
    assert set(checks) <= {"size", "ext", "time"}


def print_passed_vs_failed(report: list[dict[str, Any]]) -> None:
    failed = sum(True for request in report if request["tracebacks"])
    passed = len(report) - failed
    failed_perc = failed * 100 / len(report)
    passed_perc = passed * 100 / len(report)
    if failed:
        typer.secho(f"FAILED: {failed} ({failed_perc:.1f}%)", fg=typer.colors.RED)
    if passed:
        typer.secho(f"PASSED: {passed} ({passed_perc:.1f}%)", fg=typer.colors.GREEN)
