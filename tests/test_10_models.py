from pathlib import Path

import pytest

from cads_e2e_tests import models
from cads_e2e_tests.models import Checks, Report, Request


@pytest.fixture
def report() -> Report:
    return Report(
        request=Request(
            collection_id="foo",
            checks=Checks(checksum="foo", extension=".foo", size=0, time=0.1),
        ),
        checksum="bar",
        extension=".bar",
        size=1,
        time=1,
    )


def test_report_run_checks(report: Report) -> None:
    expected_tracebacks = [
        "cads_e2e_tests.exceptions.ChecksumError: actual='bar' expected='foo'",
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
    expected_reports = [report]
    report_path = tmp_path / "report.json"
    models.dump_reports(reports=expected_reports, fp=report_path.open("w"))

    actual_reports = models.load_reports(report_path.open())
    assert actual_reports == expected_reports


def test_dump_and_load_requests(report: Report, tmp_path: Path) -> None:
    expected_requests = [report.request]
    requests_path = tmp_path / "requests.yaml"
    models.dump_requests(expected_requests, requests_path.open("w"))
    actual_requests = models.load_requests(requests_path.open("r"))
    assert actual_requests == expected_requests
