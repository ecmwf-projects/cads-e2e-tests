import json
from pathlib import Path

from pytest import CaptureFixture

from cads_e2e_tests.cli import make_report

REQUESTS_YAML = """# requests.yaml
- collection_id: test-adaptor-dummy
  parameters:
    size: 0
  checks:
    ext: .grib
    size: 0
    time: 60
    checksum: d41d8cd98f00b204e9800998ecf8427e
"""


def test_cli_make_report_from_yaml(
    capsys: CaptureFixture[str], key: str, url: str, tmp_path: Path
) -> None:
    requests_path = tmp_path / "requests.yaml"
    requests_path.write_text(REQUESTS_YAML)
    report_path = tmp_path / "report.json"

    make_report(
        key=key,
        url=url,
        requests_path=str(requests_path),
        report_path=str(report_path),
    )

    captured = capsys.readouterr()
    assert captured.out == "PASSED: 1 (100.0%)\n"

    (actual_report,) = json.load(report_path.open())
    expected_report = {
        "collection_id": "test-adaptor-dummy",
        "parameters": {
            "size": 0,
            "_timestamp": actual_report["parameters"]["_timestamp"],
        },
        "checks": {
            "ext": ".grib",
            "size": 0,
            "time": 60,
            "checksum": "d41d8cd98f00b204e9800998ecf8427e",
        },
        "checksum": "d41d8cd98f00b204e9800998ecf8427e",
        "tracebacks": [],
        "request_uid": actual_report["request_uid"],
        "target": actual_report["target"],
        "size": 0,
        "elapsed_time": actual_report["elapsed_time"],
    }
    assert actual_report == expected_report
