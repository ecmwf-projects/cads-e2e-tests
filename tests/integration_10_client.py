from __future__ import annotations

import datetime
import uuid
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from cads_e2e_tests.client import TestClient


def test_client_report(client: TestClient) -> None:
    request = {"collection_id": "test-adaptor-dummy", "size": 1}
    start = datetime.datetime.now()
    (actual_report,) = client.make_report(requests=[request])
    end = datetime.datetime.now()

    _timestamp = actual_report["request"].pop("_timestamp")
    timestamp = datetime.datetime.fromisoformat(_timestamp)
    assert start < timestamp < end

    request_uid = actual_report.pop("request_uid")
    assert uuid.UUID(request_uid)

    target = actual_report.pop("target")
    assert target.endswith(".grib")

    elapsed_time = actual_report.pop("elapsed_time")
    assert isinstance(elapsed_time, float)

    expected_report = {
        "collection_id": "test-adaptor-dummy",
        "request": {"size": 1},
        "tracebacks": [],
        "size": 1,
    }
    assert actual_report == expected_report


def test_expected_ext(client: TestClient) -> None:
    request = {"collection_id": "test-adaptor-dummy", "expected_ext": ".foo"}
    (report,) = client.make_report(requests=[request])
    (traceback,) = report["tracebacks"]
    assert "AssertionError: ext='.grib' expected_ext='.foo'" in traceback


def test_expected_size(client: TestClient) -> None:
    request = {"collection_id": "test-adaptor-dummy", "size": 1, "expected_size": 2}
    (report,) = client.make_report(requests=[request])
    (traceback,) = report["tracebacks"]
    assert "AssertionError: size=1 expected_size=2" in traceback


def test_expected_time(client: TestClient) -> None:
    request = {"collection_id": "test-adaptor-dummy", "expected_time": 0}
    (report,) = client.make_report(requests=[request])
    elapsed_time = report["elapsed_time"]
    (traceback,) = report["tracebacks"]
    assert f"AssertionError: {elapsed_time=} expected_time=0" in traceback
