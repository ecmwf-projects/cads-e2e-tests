import datetime
import json
import re
import uuid
from pathlib import Path
from typing import Any

import pytest

from cads_e2e_tests.client import TestClient


def test_client_make_report(client: TestClient) -> None:
    request = {"collection_id": "test-adaptor-dummy", "parameters": {"size": 1}}
    start = datetime.datetime.now()
    (actual_report,) = client.make_report(requests=[request])
    end = datetime.datetime.now()

    _timestamp = actual_report["parameters"]["_timestamp"]
    timestamp = datetime.datetime.fromisoformat(_timestamp)
    assert start < timestamp < end

    request_uid = actual_report["request_uid"]
    assert uuid.UUID(request_uid)

    target = actual_report["target"]
    assert target.endswith(".grib")

    elapsed_time = actual_report["elapsed_time"]
    assert isinstance(elapsed_time, float)

    expected_report = {
        "collection_id": "test-adaptor-dummy",
        "parameters": {"size": 1, "_timestamp": _timestamp},
        "checks": {},
        "tracebacks": [],
        "request_uid": request_uid,
        "target": target,
        "size": 1,
        "elapsed_time": elapsed_time,
    }
    assert actual_report == expected_report


@pytest.mark.parametrize(
    "cache,expected_parameters",
    [
        (True, {"size"}),
        (False, {"size", "_timestamp"}),
    ],
)
def test_client_cache(
    client: TestClient, cache: bool, expected_parameters: set[str]
) -> None:
    request = {"collection_id": "test-adaptor-dummy", "parameters": {"size": 1}}
    (report,) = client.make_report(requests=[request], cache=cache)
    actual_parameters = set(report["parameters"])
    assert actual_parameters == expected_parameters


def test_client_write_report(client: TestClient, tmp_path: Path) -> None:
    request = {"collection_id": "test-adaptor-dummy"}
    report_path = tmp_path / "report.json"
    expected_report = client.make_report(
        requests=[request],
        report_path=report_path,
    )
    actual_report = json.load(report_path.open())
    assert expected_report == actual_report


@pytest.mark.parametrize(
    "checks,expected_error",
    [
        ({"ext": ".foo"}, r"AssertionError: '.grib' != '.foo'"),
        ({"size": 2}, r"AssertionError: 1 != 2"),
        ({"time": 0.0}, r"AssertionError: .* > 0.0"),
    ],
)
def test_client_checks(
    client: TestClient, checks: dict[str, Any], expected_error: str
) -> None:
    request = {
        "collection_id": "test-adaptor-dummy",
        "parameters": {"size": 1},
        "checks": checks,
    }
    (report,) = client.make_report(requests=[request])
    assert report["checks"] == checks

    (traceback,) = report["tracebacks"]
    *_, actual_error = traceback.splitlines()
    assert re.compile(expected_error).match(actual_error)


def test_client_random_request(client: TestClient) -> None:
    request = {"collection_id": "test-adaptor-url"}
    (report,) = client.make_report(requests=[request])
    assert len(report["parameters"]) > 1
    assert not report["tracebacks"]


def test_client_no_requests(key: str, url: str) -> None:
    class MockClient(TestClient):
        @property
        def collecion_ids(self) -> list[str]:
            return ["test-adaptor-url"]

    client = MockClient(key=key, url=url)
    (report,) = client.make_report(requests=None)
    assert len(report["parameters"]) > 1
    assert not report["tracebacks"]
