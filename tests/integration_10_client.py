import datetime
import re
import uuid
from pathlib import Path

import pytest

from cads_e2e_tests.client import TestClient
from cads_e2e_tests.models import Checks, Report, Request, load_reports


@pytest.fixture
def dummy_request() -> Request:
    return Request(
        collection_id="test-adaptor-dummy",
        parameters={"size": 0},
    )


@pytest.mark.parametrize("download", [True, False])
def test_client_make_reports(
    client: TestClient, dummy_request: Request, download: bool
) -> None:
    (actual_report,) = client.make_reports(
        requests=[dummy_request], cache_key=None, download=download
    )

    started_at = actual_report.started_at
    assert isinstance(started_at, datetime.datetime)

    finished_at = actual_report.finished_at
    assert isinstance(finished_at, datetime.datetime)

    assert (finished_at - started_at).total_seconds() >= 1

    request_uid = actual_report.request_uid
    assert uuid.UUID(request_uid)

    time = actual_report.time
    assert isinstance(time, float)
    assert time > 0

    expected_report = Report(
        request=Request(
            collection_id="test-adaptor-dummy",
            parameters={"size": 0},
        ),
        started_at=started_at,
        finished_at=finished_at,
        request_uid=request_uid,
        extension=".grib" if download else None,
        size=0 if download else None,
        checksum="d41d8cd98f00b204e9800998ecf8427e" if download else None,
        time=time,
        content_length=0,
        content_type="application/x-grib",
    )
    assert actual_report == expected_report


@pytest.mark.parametrize(
    "cache_key,expected_parameters",
    [
        (None, {"size"}),
        ("nocache", {"size", "nocache"}),
    ],
)
def test_client_cache(
    client: TestClient,
    dummy_request: Request,
    cache_key: str | None,
    expected_parameters: set[str],
) -> None:
    (report,) = client.make_reports(
        requests=[dummy_request],
        cache_key=cache_key,
    )
    actual_parameters = set(report.request.parameters)
    assert actual_parameters == expected_parameters


def test_client_write_reports(
    client: TestClient, dummy_request: Request, tmp_path: Path
) -> None:
    report_path = tmp_path / "report.json"
    expected_report = client.make_reports(
        requests=[dummy_request],
        reports_path=report_path,
        cache_key=None,
    )
    actual_report = load_reports(report_path.open())
    assert expected_report == actual_report


@pytest.mark.parametrize(
    "checks,expected_error",
    [
        (
            Checks(checksum="foo"),
            (
                r"cads_e2e_tests.exceptions.ChecksumError: "
                r"actual='d41d8cd98f00b204e9800998ecf8427e' expected='foo'"
            ),
        ),
        (
            Checks(content_length=1),
            r"cads_e2e_tests.exceptions.ContentLengthError: actual=0 expected=1",
        ),
        (
            Checks(content_type="foo"),
            r"cads_e2e_tests.exceptions.ContentTypeError: actual='application/x-grib' expected='foo'",
        ),
        (
            Checks(extension=".foo"),
            r"cads_e2e_tests.exceptions.ExtensionError: actual='.grib' expected='.foo'",
        ),
        (
            Checks(size=1),
            r"cads_e2e_tests.exceptions.SizeError: actual=0 expected=1",
        ),
        (
            Checks(time=0.0),
            r"cads_e2e_tests.exceptions.TimeError: actual=.* expected=0.0",
        ),
    ],
)
def test_client_checks(
    client: TestClient, dummy_request: Request, checks: Checks, expected_error: str
) -> None:
    request_with_checks = Request(
        checks=checks, **dummy_request.model_dump(exclude={"checks"})
    )

    (report,) = client.make_reports(requests=[request_with_checks], cache_key=None)
    assert report.request.checks == checks

    print(report.tracebacks)
    (traceback,) = report.tracebacks
    *_, actual_error = traceback.splitlines()
    assert re.compile(expected_error).match(actual_error)


def test_client_random_request(client: TestClient) -> None:
    request = Request(collection_id="test-adaptor-url")
    (report,) = client.make_reports(requests=[request], cache_key=None)
    assert len(report.request.parameters) > 1
    assert not report.tracebacks


def test_client_random_request_widgets(client: TestClient) -> None:
    request = Request(collection_id="test-layout-sandbox-nogecko-dataset")
    (report,) = client.make_reports(requests=[request], cache_key=None)
    assert report.request.parameters

    required_fields = {
        widget["name"]
        for widget in client.get_collection("test-layout-sandbox-nogecko-dataset").form
        if widget.get("required")
    }
    assert required_fields <= set(report.request.parameters)

    (traceback,) = report.tracebacks
    assert traceback.endswith("UrlNoDataError\n")


def test_client_no_requests(key: str, url: str) -> None:
    class MockClient(TestClient):
        @property
        def collection_ids(self) -> list[str]:
            return ["test-adaptor-url"]

    client = MockClient(key=key, url=url)
    (report,) = client.make_reports(requests=None, cache_key=None)
    assert len(report.request.parameters) > 1
    assert not report.tracebacks


def test_client_regex_pattern(client: TestClient, dummy_request: Request) -> None:
    requests = [dummy_request, Request(collection_id="foo")]
    (report,) = client.make_reports(
        requests=requests, cache_key=None, regex_pattern="test-*"
    )
    assert report.request.collection_id == "test-adaptor-dummy"


def test_client_unreachable_collection(client: TestClient) -> None:
    request = Request(collection_id="foo")
    (report,) = client.make_reports(requests=[request], cache_key=None)
    (traceback,) = report.tracebacks
    assert "404 Client Error" in traceback


@pytest.mark.parametrize("n_repeats", [1, 2])
def test_n_repeats(client: TestClient, dummy_request: Request, n_repeats: int) -> None:
    reports = client.make_reports(requests=[dummy_request], n_repeats=n_repeats)
    assert len(reports) == n_repeats


def test_max_runtime(client: TestClient, dummy_request: Request) -> None:
    requests = [dummy_request]
    (report,) = client.make_reports(requests=requests, max_runtime=10)
    assert not report.tracebacks

    (report,) = client.make_reports(requests=requests, max_runtime=0)
    (traceback,) = report.tracebacks
    assert traceback.endswith("TimeoutError: Maximum runtime exceeded.\n")
