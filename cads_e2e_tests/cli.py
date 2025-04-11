from typing import Annotated, Optional

import typer
from typer import Option

from . import models, reporter
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
    url: Annotated[Optional[str], Option(help="API url")] = None,  # noqa: UP007
    key: Annotated[list[str], Option(help="API key(s)")] = [],
    requests_path: Annotated[
        Optional[str],  # noqa: UP007
        Option(
            help="Path to the YAML file with requests to test",
            show_default="random requests",
        ),
    ] = None,
    reports_path: Annotated[
        str, Option(help="Path to write the reports in JSON Lines format")
    ] = "reports.jsonl",
    invalidate_cache: Annotated[
        bool,
        Option(help="Whether to invalidate the cache"),
    ] = True,
    n_jobs: Annotated[
        int,
        Option(help="Number of concurrent requests"),
    ] = 1,
    verbose: Annotated[
        int,
        Option(help="The verbosity level of joblib"),
    ] = 10,
    log_level: Annotated[
        str,
        Option(help="Set the root logger level to the specified level"),
    ] = "INFO",
    regex_pattern: Annotated[
        str,
        Option(help="Regex pattern used to filter collection IDs"),
    ] = r"^(?!test-|provider-).*(?<!-complete)$",
    download: Annotated[
        bool,
        Option(help="Whether to download the results"),
    ] = True,
    cache_key: Annotated[
        str,
        Option(help="Key used to invalidate the cache"),
    ] = "_no_cache",
    n_repeats: Annotated[
        int,
        Option(
            help="Number of times to repeat each request (random requests are regenerated)"
        ),
    ] = 1,
    cyclic: Annotated[
        bool,
        Option(
            help="Whether to repeat requests cyclically ([1, 2, 1, 2]) or not ([1, 1, 2, 2])"
        ),
    ] = True,
    randomise: Annotated[
        bool,
        Option(help="Whether to randomise the order of the requests"),
    ] = False,
    max_runtime: Annotated[
        float | None,
        Option(help="Maximum time (in seconds) each request is allowed to run"),
    ] = None,
    datapi_maximum_tries: Annotated[
        int,
        Option(help="Maximum number of retries"),
    ] = 1,
) -> None:
    """CADS E2E Tests."""
    if requests_path is not None:
        with open(requests_path, "r") as fp:
            requests = models.load_requests(fp)
    else:
        requests = None

    reports = list(
        reporter.reports_generator(
            url=url,
            keys=key,
            requests=requests,
            reports_path=reports_path,
            cache_key=cache_key if invalidate_cache else None,
            n_jobs=n_jobs,
            verbose=verbose,
            regex_pattern=regex_pattern,
            download=download,
            n_repeats=n_repeats,
            cyclic=cyclic,
            randomise=randomise,
            max_runtime=max_runtime,
            log_level=log_level,
            maximum_tries=datapi_maximum_tries,
        )
    )
    echo_passed_vs_failed(reports)
