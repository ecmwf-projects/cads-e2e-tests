from typing import Optional

import yaml

from .client import TestClient


def make_report(
    url: Optional[str] = None,  # noqa: UP007
    key: Optional[str] = None,  # noqa: UP007
    requests_path: Optional[str] = None,  # noqa: UP007
    report_path: str = "e2e_report.json",
) -> None:
    if requests_path is not None:
        with open(requests_path, "r") as fp:
            requests = yaml.safe_load(fp)
    else:
        requests = None

    client = TestClient(url=url, key=key)
    client.make_report(requests=requests, report_path=report_path)
