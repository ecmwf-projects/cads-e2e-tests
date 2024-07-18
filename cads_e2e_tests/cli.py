from typing import Annotated, Optional

import yaml
from typer import Option

from . import utils
from .client import TestClient


def make_report(
    url: Annotated[Optional[str], Option(help="CADS api url")] = None,  # noqa: UP007
    key: Annotated[Optional[str], Option(help="CADS api key")] = None,  # noqa: UP007
    requests_path: Annotated[
        Optional[str],  # noqa: UP007
        Option(
            help="Path to the YAML file with requests to test",
            show_default="random requests",
        ),
    ] = None,
    report_path: Annotated[
        str, Option(help="Path to write the report in JSON format")
    ] = "report.json",
) -> None:
    """CADS E2E Tests."""
    if requests_path is not None:
        with open(requests_path, "r") as fp:
            requests = yaml.safe_load(fp)
    else:
        requests = None

    client = TestClient(url=url, key=key)
    report = client.make_report(requests=requests, report_path=report_path)
    utils.print_passed_vs_failed(report)
