from typing import Optional

import typer

from .client import Client


def cli(
    url: Optional[str] = None,
    key: Optional[str] = None,
    report_path: str = "e2e_report.json",
) -> None:
    client = Client(url=url, key=key)
    client.test_random_requests(report_path=report_path)


def main() -> None:
    typer.run(cli)


if __name__ == "__main__":
    main()
