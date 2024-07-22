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
    target = utils.Target(str(tmp_file))

    assert target.checksum == "acbd18db4cc2f85cedef654fccc4a4d8"
    assert target.size == 3
    assert target.extension == ".txt"
