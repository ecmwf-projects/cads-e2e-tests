from pathlib import Path

import pytest

from cads_e2e_tests import models
from cads_e2e_tests.models import Checks, Report, Request


@pytest.fixture
def report() -> Report:
    return Report(
        request=Request(
            collection_id="foo",
            checks=Checks(
                checksum="foo",
                extension=".foo",
                size=0,
                time=0.1,
                content_length=10,
                content_type="foo-type",
            ),
        ),
        checksum="bar",
        extension=".bar",
        size=1,
        time=1,
        content_length=20,
        content_type="bar-type",
    )


def test_report_run_checks(report: Report) -> None:
    expected_tracebacks = [
        "cads_e2e_tests.exceptions.ChecksumError: actual='bar' expected='foo'",
        "cads_e2e_tests.exceptions.ContentLengthError: actual=20 expected=10",
        "cads_e2e_tests.exceptions.ContentTypeError: actual='bar-type' expected='foo-type'",
        "cads_e2e_tests.exceptions.ExtensionError: actual='.bar' expected='.foo'",
        "cads_e2e_tests.exceptions.SizeError: actual=1 expected=0",
        "cads_e2e_tests.exceptions.TimeError: actual=1.0 expected=0.1",
    ]

    actual_tracebacks = [
        traceback.splitlines()[-1] for traceback in report.run_checks()
    ]
    assert expected_tracebacks == actual_tracebacks
    assert report.tracebacks == []


def test_dump_and_load_reports(report: Report, tmp_path: Path) -> None:
    report_path = tmp_path / "report.jsonl"
    with report_path.open("a") as fp:
        models.dump_report(report, fp)
        models.dump_report(report, fp)

    assert models.load_reports(report_path.open()) == [report, report]


def test_dump_and_load_requests(report: Report, tmp_path: Path) -> None:
    expected_requests = [report.request]
    requests_path = tmp_path / "requests.yaml"
    models.dump_requests(expected_requests, requests_path.open("w"))
    actual_requests = models.load_requests(requests_path.open("r"))
    assert actual_requests == expected_requests
