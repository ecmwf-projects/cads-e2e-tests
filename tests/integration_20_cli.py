from pathlib import Path

from pytest import CaptureFixture

from cads_e2e_tests import models
from cads_e2e_tests.cli import make_reports
from cads_e2e_tests.models import Checks, Report, Request

REQUESTS_YAML = """# requests.yaml
- collection_id: test-adaptor-dummy
  parameters:
    size: 0
  checks:
    extension: .grib
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

    make_reports(
        key=key,
        url=url,
        requests_path=str(requests_path),
        reports_path=str(report_path),
        invalidate_cache=False,
    )

    captured = capsys.readouterr()
    assert captured.out == "PASSED: 1 (100.0%)\n"

    (actual_report,) = models.load_reports(report_path.open())
    expected_report = Report(
        request=Request(
            collection_id="test-adaptor-dummy",
            parameters={"size": 0},
            checks=Checks(
                extension=".grib",
                size=0,
                time=60,
                checksum="d41d8cd98f00b204e9800998ecf8427e",
            ),
        ),
        request_uid=actual_report.request_uid,
        extension=".grib",
        size=0,
        checksum="d41d8cd98f00b204e9800998ecf8427e",
        time=actual_report.time,
    )

    assert actual_report == expected_report
