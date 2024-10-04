from typing import Any

import pytest

from cads_e2e_tests import cli
from cads_e2e_tests.models import Report, Request


def test_echo_passed_vs_failed(capsys: pytest.CaptureFixture[Any]) -> None:
    request = Request(collection_id="foo")
    report: list[Report] = [
        Report(request=request),
        Report(request=request, tracebacks=["foo"]),
        Report(request=request),
    ]
    cli.echo_passed_vs_failed(report)
    captured = capsys.readouterr()
    assert (
        captured.out == "NUMBER OF REPORTS: 3\nFAILED: 1 (33.3%)\nPASSED: 2 (66.7%)\n"
    )
