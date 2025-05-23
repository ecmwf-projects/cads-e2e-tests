import contextlib
import logging
import os
from pathlib import Path

import pytest

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


@pytest.mark.parametrize(
    "cyclic,randomise,expected",
    [
        (True, False, [[1, 2, 1, 2]]),
        (False, False, [[1, 1, 2, 2]]),
        (True, True, [[1, 2, 1, 2], [2, 1, 2, 1], [1, 2, 2, 1], [2, 1, 1, 2]]),
        (False, True, [[1, 1, 2, 2], [2, 2, 1, 1]]),
    ],
)
def test_reorder(cyclic: bool, randomise: bool, expected: set[list[int]]) -> None:
    for _ in range(100):
        actual = utils.reorder([1, 2], cyclic=cyclic, randomise=randomise, n_repeats=2)
        assert actual in expected


def test_random_choiche_from_range() -> None:
    for _ in range(100):
        assert utils.random_choice_from_range(0, 0.2, 0.1) in [0, 0.1, 0.2]
