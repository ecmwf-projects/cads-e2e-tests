import datetime
import uuid

from cads_e2e_tests.client import TestClient


def test_report(client: TestClient) -> None:
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


def test_random_request(client: TestClient) -> None:
    request = {"collection_id": "test-adaptor-url"}
    (report,) = client.make_report(requests=[request])
    expected_parameters = {
        "_timestamp",
        "month",
        "reference_dataset",
        "variable",
        "version",
        "year",
    }
    actual_parameters = set(report["request"])
    assert expected_parameters == actual_parameters


def test_no_requests(key: str, url: str) -> None:
    class MockClient(TestClient):
        @property
        def collecion_ids(self) -> list[str]:
            return ["test-adaptor-url"]

    client = MockClient(key=key, url=url)
    (report,) = client.make_report(requests=None)
    expected_parameters = {
        "_timestamp",
        "month",
        "reference_dataset",
        "variable",
        "version",
        "year",
    }
    actual_parameters = set(report["request"])
    assert expected_parameters == actual_parameters
