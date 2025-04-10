import itertools
import re
from typing import Any, Iterator, Sequence

import joblib

from . import utils
from .client import TestClient
from .models import Checks, Report, Request

DOWNLOAD_CHECKS = {"checksum", "extension", "size"}
REQUESTS_DEFAULT = None


def _switch_off_download_checks(request: Request) -> Request:
    checks_dict = dict.fromkeys(DOWNLOAD_CHECKS, None)
    checks = Checks(
        **checks_dict,
        **request.checks.model_dump(exclude=DOWNLOAD_CHECKS),
    )
    return Request(checks=checks, **request.model_dump(exclude={"checks"}))


def reports_generator(
    url: str | None,
    keys: list[str],
    requests: Sequence[Request] | None = None,
    cache_key: str | None = None,
    n_jobs: int = 1,
    verbose: int = 0,
    regex_pattern: str = "",
    download: bool = True,
    n_repeats: int = 1,
    cyclic: bool = True,
    randomise: bool = False,
    max_runtime: float | None = None,
    log_level: str | None = None,
    **kwargs: Any,
) -> Iterator[Report]:
    clients = [
        TestClient(url=url, key=key, **kwargs) for key in ([None] if not keys else keys)
    ]
    for client in clients:
        client.accept_all_missing_licences()

    if requests is None:
        requests = [
            Request(collection_id=collection_id)
            for collection_id in clients[0].collection_ids
        ]

    requests = [
        request
        for request in requests
        if re.search(regex_pattern, request.collection_id)
    ]

    if not download:
        requests = [_switch_off_download_checks(request) for request in requests]

    requests = utils.reorder(
        requests,
        cyclic=cyclic,
        randomise=randomise,
        n_repeats=n_repeats,
    )

    parallel = joblib.Parallel(
        n_jobs=n_jobs, verbose=verbose, return_as="generator_unordered"
    )
    reports: Iterator[Report] = parallel(
        client.delayed_make_report(
            request=request,
            cache_key=cache_key,
            download=download,
            max_runtime=max_runtime,
            log_level=log_level,
        )
        for client, request in zip(itertools.cycle(clients), requests)
    )
    return reports
