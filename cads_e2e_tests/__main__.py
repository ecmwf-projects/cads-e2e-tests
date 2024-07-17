from typing import Optional

import typer
import yaml

from .client import TestClient


def cli(
    url: Optional[str] = None,  # noqa: UP007
    key: Optional[str] = None,  # noqa: UP007
    requests_path: Optional[str] = None,  # noqa: UP007
    report_path: str = "e2e_report.json",
) -> None:
    client = TestClient(url=url, key=key)

    if requests_path is not None:
        with open(requests_path, "r") as fp:
            requests = yaml.safe_load(fp)
    else:
        requests = None

    client.make_report(requests=requests, report_path=report_path)


def main() -> None:
    typer.run(cli)


if __name__ == "__main__":
    main()
