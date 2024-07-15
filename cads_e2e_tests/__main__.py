from typing import Optional

import typer

from .client import Client


def cli(
    url: Optional[str] = None,  # noqa: UP007
    key: Optional[str] = None,  # noqa: UP007
    report_path: str = "e2e_report.json",
    requests_yaml_path: Optional[str] = None,  # noqa: UP007
) -> None:
    client = Client(url=url, key=key)
    client.write_report(report_path=report_path, requests_yaml_path=requests_yaml_path)


def main() -> None:
    typer.run(cli)


if __name__ == "__main__":
    main()
