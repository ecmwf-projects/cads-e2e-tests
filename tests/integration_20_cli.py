from pathlib import Path

import pytest
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
    content_length: 0
    content_type: application/x-grib
"""


@pytest.mark.parametrize(
    "download,extension,size,checksum",
    [(True, ".grib", 0, "d41d8cd98f00b204e9800998ecf8427e"), (False, None, None, None)],
)
def test_cli_make_report_from_yaml(
    capsys: CaptureFixture[str],
    key: str,
    url: str,
    tmp_path: Path,
    download: bool,
    extension: str | None,
    size: int | None,
    checksum: str | None,
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
        regex_pattern="",
        download=download,
    )

    captured = capsys.readouterr()
    assert captured.out == "NUMBER OF REPORTS: 1\nPASSED: 1 (100.0%)\n"

    (actual_report,) = models.load_reports(report_path.open())
    expected_report = Report(
        request=Request(
            collection_id="test-adaptor-dummy",
            parameters={"size": 0},
            checks=Checks(
                extension=extension,
                size=size,
                time=60,
                checksum=checksum,
                content_length=0,
                content_type="application/x-grib",
            ),
        ),
        request_uid=actual_report.request_uid,
        extension=extension,
        size=size,
        checksum=checksum,
        time=actual_report.time,
        content_length=0,
        content_type="application/x-grib",
    )

    assert actual_report == expected_report
