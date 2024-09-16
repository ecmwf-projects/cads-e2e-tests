from typing import Annotated, Optional

import typer
from typer import Option

from . import models
from .client import TestClient
from .models import Report


def echo_passed_vs_failed(reports: list[Report]) -> None:
    failed = sum(True for request in reports if request.tracebacks)
    passed = len(reports) - failed
    failed_perc = failed * 100 / len(reports)
    passed_perc = passed * 100 / len(reports)
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
    collection_pattern_match: Annotated[
        Optional[str],  # noqa: UP007
        Option(
            help=(
                "A string pattern to match collections when exectuing random requests. "
                "For example 'reanalysis' will run tests for collections with 'reanalysis' in the id"
            ),
        ),
    ] = "",
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
        collection_pattern_match=collection_pattern_match,
    )
    echo_passed_vs_failed(reports)
