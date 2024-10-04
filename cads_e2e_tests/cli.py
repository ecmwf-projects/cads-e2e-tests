from typing import Annotated, Optional

import typer
from typer import Option

from . import models
from .client import TestClient
from .models import Report


def echo_passed_vs_failed(reports: list[Report]) -> None:
    n_reports = len(reports)
    typer.secho(
        f"NUMBER OF REPORTS: {n_reports}",
        fg=typer.colors.YELLOW if not n_reports else None,
    )
    if n_reports:
        failed = sum(True for request in reports if request.tracebacks)
        passed = len(reports) - failed
        failed_perc = failed * 100 / n_reports
        passed_perc = passed * 100 / n_reports
        if failed:
            typer.secho(f"FAILED: {failed} ({failed_perc:.1f}%)", fg=typer.colors.RED)
        if passed:
            typer.secho(f"PASSED: {passed} ({passed_perc:.1f}%)", fg=typer.colors.GREEN)


def make_reports(
    url: Annotated[Optional[str], Option(help="CADS api url")] = None,  # noqa: UP007
    key: Annotated[Optional[str], Option(help="CADS api key")] = None,  # noqa: UP007
    requests_path: Annotated[
        Optional[str],  # noqa: UP007
        Option(
            help="Path to the YAML file with requests to test",
            show_default="random requests",
        ),
    ] = None,
    reports_path: Annotated[
        str, Option(help="Path to write the reports in JSON format")
    ] = "reports.json",
    invalidate_cache: Annotated[
        bool,
        Option(help="Whether to invalidate the cache using the _timestamp parameter"),
    ] = True,
    n_jobs: Annotated[
        int,
        Option(help="Number of concurrent requests"),
    ] = 1,
    verbose: Annotated[
        int,
        Option(help="The verbosity level"),
    ] = 10,
    regex_pattern: Annotated[
        str,
        Option(help="Regex pattern used to filter collection IDs"),
    ] = r"^(?!test-|provider-).*(?<!-complete)$",
    dont_download: Annotated[
        bool,
        Option(help="Whether to download the files"),
    ] = False,
) -> None:
    """CADS E2E Tests."""
    if requests_path is not None:
        with open(requests_path, "r") as fp:
            requests = models.load_requests(fp)
    else:
        requests = None

    client = TestClient(url=url, key=key, maximum_tries=0)
    reports = client.make_reports(
        requests=requests,
        reports_path=reports_path,
        invalidate_cache=invalidate_cache,
        n_jobs=n_jobs,
        verbose=verbose,
        regex_pattern=regex_pattern,
        dont_download=dont_download
    )
    echo_passed_vs_failed(reports)
