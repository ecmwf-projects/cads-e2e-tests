import contextlib
import logging
import os
from pathlib import Path
from typing import Any

import pytest

from cads_e2e_tests import exceptions, utils

does_not_raise = contextlib.nullcontext


def test_utils_catch_exception() -> None:
    def raise_foo_error() -> None:
        raise ValueError("foo")

    report: dict[str, Any] = {"tracebacks": []}

    with utils.catch_exception(
        report,
        logger=logging.getLogger(),
        elapsed_time=False,
        allowed_exceptions=(ValueError,),
    ):
        raise_foo_error()

    (traceback,) = report["tracebacks"]
    assert traceback.endswith("ValueError: foo\n")

    with utils.catch_exception(
        report,
        logger=logging.getLogger(),
        elapsed_time=True,
        allowed_exceptions=(ValueError,),
    ) as tic:
        assert isinstance(tic, float)
    assert 0 <= report["elapsed_time"] <= 1


def test_utils_tmp_working_dir() -> None:
    with utils.tmp_working_dir() as tmpdir:
        assert os.getcwd() == os.path.realpath(tmpdir)
    assert not os.path.exists(tmpdir)


@pytest.mark.parametrize(
    "ext,size,time,checksum,raises",
    [
        (
            ".bar",
            1,
            2,
            "foo",
            pytest.raises(exceptions.ExtensionError, match="'.foo' != '.bar'"),
        ),
        (
            ".foo",
            0,
            2,
            "foo",
            pytest.raises(exceptions.SizeError, match="1 != 0"),
        ),
        (
            ".foo",
            1,
            0,
            "foo",
            pytest.raises(exceptions.TimeError, match="2 > 0"),
        ),
        (
            ".foo",
            1,
            2,
            "bar",
            pytest.raises(exceptions.ChecksumError, match="'foo' != 'bar'"),
        ),
        (
            None,
            None,
            None,
            None,
            does_not_raise(),
        ),
    ],
)
def test_utils_check_report(
    ext: str,
    size: int,
    time: float,
    checksum: str,
    raises: contextlib.nullcontext[Any],
) -> None:
    report = {"target": "file.foo", "size": 1, "elapsed_time": 2, "checksum": "foo"}
    with raises:
        assert utils.check_report(report, ext, size, time, checksum) == report


@pytest.mark.parametrize(
    "request_dict",
    (
        [],
        {"foo": "bar"},
        {"collection_id": "foo", "foo": "bar"},
        {"collection_id": 1},
        {"collection_id": "foo", "parameters": 1},
        {"collection_id": "foo", "checks": 1},
        {"collection_id": "foo", "checks": {"foo": "bar"}},
    ),
)
def test_utils_validate_request(request_dict: dict[str, Any]) -> None:
    with pytest.raises(AssertionError):
        utils.validate_request(request_dict)


def test_get_target_info(tmp_path: Path) -> None:
    tmp_file = tmp_path / "test.txt"
    tmp_file.write_text("foo")
    target = str(tmp_file)
    info = utils.get_target_info(target)
    assert info == {
        "checksum": "acbd18db4cc2f85cedef654fccc4a4d8",
        "size": 3,
        "target": target,
    }


def test_print_passed_vs_failed(capsys: pytest.CaptureFixture[Any]) -> None:
    report: list[dict[str, Any]] = [
        {"tracebacks": ["foo"]},
        {"tracebacks": []},
        {"tracebacks": []},
    ]
    utils.print_passed_vs_failed(report)
    captured = capsys.readouterr()
    assert captured.out == "FAILED: 1 (33.3%)\nPASSED: 2 (66.7%)\n"
