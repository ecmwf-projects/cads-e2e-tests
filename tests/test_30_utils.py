import contextlib
import logging
import os
from pathlib import Path

from cads_e2e_tests import utils

does_not_raise = contextlib.nullcontext


def test_utils_tmp_working_dir() -> None:
    with utils.tmp_working_dir() as tmpdir:
        assert os.getcwd() == os.path.realpath(tmpdir)
    assert not os.path.exists(tmpdir)


def test_utils_catch_exceptions() -> None:
    def raise_foo_error() -> None:
        raise ValueError("foo")

    tracebacks: list[str] = []

    with utils.catch_exceptions(
        tracebacks,
        logger=logging.getLogger(),
        exceptions=(ValueError,),
    ):
        raise_foo_error()

    (traceback,) = tracebacks
    assert traceback.endswith("ValueError: foo\n")


def test_utils_target(tmp_path: Path) -> None:
    tmp_file = tmp_path / "test.txt"
    tmp_file.write_text("foo")
    target_info = utils.TargetInfo(str(tmp_file))

    assert target_info.checksum == "acbd18db4cc2f85cedef654fccc4a4d8"
    assert target_info.size == 3
    assert target_info.extension == ".txt"


def test_utils_random_date() -> None:
    assert utils.random_date("2000-01-01", "2000-01-01") == "2000-01-01"
    assert utils.random_date("2000-01-01", "2000-01-03") in [
        "2000-01-01",
        "2000-01-02",
        "2000-01-03",
    ]
