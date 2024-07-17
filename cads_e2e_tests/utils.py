import contextlib
import logging
import os
import tempfile
import time
import traceback
from typing import Any, Iterator, Type


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
    expected_ext: str | None,
    expected_size: int | None,
    expected_time: float | None,
) -> dict[str, Any]:
    if expected_ext is not None:
        _, ext = os.path.splitext(report["target"])
        assert ext == expected_ext, f"{ext=} {expected_ext=}"

    if expected_size is not None:
        size = report["size"]
        assert size == expected_size, f"{size=} {expected_size=}"

    if expected_time is not None:
        elapsed_time = report["elapsed_time"]
        assert elapsed_time <= expected_time, f"{elapsed_time=} {expected_time=}"

    return report
