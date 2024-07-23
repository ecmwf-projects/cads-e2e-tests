from typing import Any

import pytest

from cads_e2e_tests import cli
from cads_e2e_tests.models import Report, Request

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


def test_echo_passed_vs_failed(capsys: pytest.CaptureFixture[Any]) -> None:
    request = Request(collection_id="foo")
    report: list[Report] = [
        Report(request=request),
        Report(request=request, tracebacks=["foo"]),
        Report(request=request),
    ]
    cli.echo_passed_vs_failed(report)
    captured = capsys.readouterr()
    assert captured.out == "FAILED: 1 (33.3%)\nPASSED: 2 (66.7%)\n"
