import datetime
import re
import uuid

import pytest

from cads_e2e_tests import reports_generator
from cads_e2e_tests.client import TestClient
from cads_e2e_tests.models import Checks, Report, Request, Settings


@pytest.fixture
def dummy_request() -> Request:
    return Request(
        collection_id="test-adaptor-dummy",
        parameters={"size": 0},
    )


@pytest.mark.parametrize("download", [True, False])
def test_client_make_reports(
    url: str, keys: list[str], dummy_request: Request, download: bool
) -> None:
    (actual_report,) = list(
        reports_generator(
            url=url,
            keys=keys,
            requests=[dummy_request],
            cache_key=None,
            download=download,
        )
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
    url: str,
    keys: list[str],
    dummy_request: Request,
    cache_key: str | None,
    expected_parameters: set[str],
) -> None:
    (report,) = list(
        reports_generator(
            url=url,
            keys=keys,
            requests=[dummy_request],
            cache_key=cache_key,
        )
    )
    actual_parameters = set(report.request.parameters)
    assert actual_parameters == expected_parameters


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
    url: str,
    keys: list[str],
    dummy_request: Request,
    checks: Checks,
    expected_error: str,
) -> None:
    request_with_checks = Request(
        checks=checks, **dummy_request.model_dump(exclude={"checks"})
    )

    (report,) = list(
        reports_generator(
            url=url, keys=keys, requests=[request_with_checks], cache_key=None
        )
    )
    assert report.request.checks == checks

    print(report.tracebacks)
    (traceback,) = report.tracebacks
    *_, actual_error = traceback.splitlines()
    assert re.compile(expected_error).match(actual_error)


def test_client_random_request(url: str, keys: list[str]) -> None:
    request = Request(collection_id="test-adaptor-url")
    (report,) = list(
        reports_generator(url=url, keys=keys, requests=[request], cache_key=None)
    )
    assert len(report.request.parameters) > 1
    assert not report.tracebacks


@pytest.mark.parametrize("randomise", [True, False])
def test_client_partial_random_request(
    url: str, keys: list[str], randomise: bool
) -> None:
    request = Request(
        collection_id="test-adaptor-url",
        parameters={"month": ["01", "02"], "year": "1979"},
        settings=Settings(randomise=randomise),
    )
    (report,) = list(
        reports_generator(url=url, keys=keys, requests=[request], cache_key=None)
    )
    if randomise:
        assert report.request.parameters["month"] in (["01"], ["02"])
        assert report.request.parameters["year"] == ["1979"]
        assert len(report.request.parameters) > 2
        assert not report.tracebacks
    else:
        assert report.request.parameters == {"month": ["01", "02"], "year": "1979"}
        assert report.tracebacks


def test_client_random_request_widgets(url: str, keys: list[str]) -> None:
    request = Request(collection_id="test-layout-sandbox-nogecko-dataset")
    (report,) = list(
        reports_generator(url=url, keys=keys, requests=[request], cache_key=None)
    )
    parameters = report.request.parameters

    assert set(parameters) == {
        "altitude",
        "date",
        "format",
        "location",
        "max_5",
        "sky_type",
        "time_reference",
        "time_step",
    }

    assert parameters["altitude"] == -999
    assert (
        isinstance(parameters["date"], list)
        and len(parameters["date"]) == 1
        and re.match(r"^\d{4}-\d{2}-\d{2}/\d{4}-\d{2}-\d{2}$", parameters["date"][0])
    )
    assert isinstance(parameters["format"], str)
    assert set(parameters["location"]) == {"latitude", "longitude"}
    assert isinstance(parameters["max_5"], list) and len(parameters["max_5"]) == 1
    assert isinstance(parameters["sky_type"], str)
    assert isinstance(parameters["time_reference"], str)
    assert isinstance(parameters["time_step"], str)

    assert -40 <= parameters["location"]["latitude"] <= 70
    assert not parameters["location"]["latitude"] % 0.25
    assert -150 <= parameters["location"]["longitude"] <= 110
    assert not parameters["location"]["longitude"] % 0.5


def test_client_no_requests(
    monkeypatch: pytest.MonkeyPatch, url: str, keys: list[str]
) -> None:
    monkeypatch.setattr(TestClient, "collection_ids", ["test-adaptor-url"])
    (report,) = list(
        reports_generator(url=url, keys=keys, requests=None, cache_key=None)
    )
    assert len(report.request.parameters) > 1
    assert not report.tracebacks


def test_client_regex_pattern(
    url: str, keys: list[str], dummy_request: Request
) -> None:
    requests = [dummy_request, Request(collection_id="foo")]
    (report,) = list(
        reports_generator(
            url=url,
            keys=keys,
            requests=requests,
            cache_key=None,
            regex_pattern="test-*",
        )
    )
    assert report.request.collection_id == "test-adaptor-dummy"


def test_client_unreachable_collection(url: str, keys: list[str]) -> None:
    request = Request(collection_id="foo")
    (report,) = list(
        reports_generator(url=url, keys=keys, requests=[request], cache_key=None)
    )
    (traceback,) = report.tracebacks
    assert "404 Client Error" in traceback


@pytest.mark.parametrize("n_repeats", [1, 2])
def test_n_repeats(
    url: str, keys: list[str], dummy_request: Request, n_repeats: int
) -> None:
    reports = list(
        reports_generator(
            url=url, keys=keys, requests=[dummy_request], n_repeats=n_repeats
        )
    )
    assert len(reports) == n_repeats


@pytest.mark.parametrize("use_settings", [True, False])
def test_max_runtime(
    url: str, keys: list[str], dummy_request: Request, use_settings: bool
) -> None:
    if use_settings:
        dummy_request.settings = Settings(max_runtime=10)
    requests = [dummy_request]
    (report,) = list(
        reports_generator(
            url=url,
            keys=keys,
            requests=requests,
            max_runtime=None if use_settings else 10,
        )
    )
    assert not report.tracebacks

    if use_settings:
        dummy_request.settings = Settings(max_runtime=0)
    (report,) = list(
        reports_generator(
            url=url,
            keys=keys,
            requests=requests,
            max_runtime=None if use_settings else 0,
        )
    )
    (traceback,) = report.tracebacks
    assert traceback.endswith("TimeoutError: Maximum runtime exceeded.\n")


def test_client_requests_pool(
    monkeypatch: pytest.MonkeyPatch,
    url: str,
    keys: list[str],
    dummy_request: Request,
) -> None:
    monkeypatch.setattr(TestClient, "collection_ids", ["test-adaptor-dummy"])
    (report,) = list(
        reports_generator(
            url=url, keys=keys, requests=None, requests_pool=[dummy_request]
        )
    )
    assert report.request == dummy_request
