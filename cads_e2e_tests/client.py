import datetime
import functools
import logging
import os
import random
import re
from pathlib import Path
from typing import Any, Sequence

import attrs
import joblib
from cads_api_client import ApiClient
from cads_api_client.catalogue import Collections

from . import models, utils
from .models import Checks, Report, Request

LOGGER = logging.getLogger(__name__)
DOWNLOAD_CHECKS = {"checksum", "extension", "size"}


def _licences_to_set_of_tuples(
    licences: list[dict[str, Any]],
) -> set[tuple[str, int]]:
    return {(licence["id"], licence["revision"]) for licence in licences}


def _switch_off_download_checks(request: Request) -> Request:
    checks_dict = dict.fromkeys(DOWNLOAD_CHECKS, None)
    checks = Checks(
        **checks_dict,
        **request.checks.model_dump(exclude=DOWNLOAD_CHECKS),
    )
    request = Request(checks=checks, **request.model_dump(exclude={"checks"}))
    return request


@attrs.define
class TestClient(ApiClient):
    @functools.cached_property
    def missing_licences(self) -> set[tuple[str, int]]:
        licences = _licences_to_set_of_tuples(self.get_licences())
        accepted_licences = _licences_to_set_of_tuples(self.get_accepted_licences())
        return licences - accepted_licences

    @property
    def collection_ids(self) -> list[str]:
        collection_ids = []
        collections: Collections | None = self.get_collections()
        while collections is not None:
            collection_ids.extend(collections.collection_ids)
            collections = collections.next
        return collection_ids

    def random_parameters(self, collection_id: str) -> dict[str, Any]:
        parameters = self.apply_constraints(collection_id)
        for key in list(parameters):
            if choices := self.apply_constraints(collection_id, **parameters)[key]:
                parameters[key] = random.choice(choices)
            else:
                parameters.pop(key)
        return parameters

    def update_request_parameters(
        self,
        request: Request,
        invalidate_cache: bool,
    ) -> Request:
        parameters = dict(request.parameters)
        if not parameters:
            parameters = self.random_parameters(request.collection_id)
        if invalidate_cache:
            parameters.setdefault("_timestamp", datetime.datetime.now().isoformat())
        return Request(
            parameters=parameters,
            **request.model_dump(exclude={"parameters"}),
        )

    def _make_report(
        self, request: Request, invalidate_cache: bool, download: bool
    ) -> Report:
        report = Report(request=request)

        tracebacks: list[str] = []
        with utils.catch_exceptions(tracebacks, logger=LOGGER):
            request = self.update_request_parameters(request, invalidate_cache)
            report = Report(
                request=request,
                **report.model_dump(exclude={"request"}),
            )

            remote = self.submit(request.collection_id, **request.parameters)
            report = Report(
                request_uid=remote.request_uid,
                **report.model_dump(exclude={"request_uid"}),
            )

            results = remote.make_results()
            tic = datetime.datetime.fromisoformat(remote.json["started"])
            toc = datetime.datetime.fromisoformat(remote.json["finished"])
            elapsed_time = (toc - tic).total_seconds()

            report = Report(
                time=elapsed_time,
                content_length=results.content_length,
                content_type=results.content_type,
                **report.model_dump(exclude={"time", "content_length", "content_type"}),
            )
            if download:
                results_info = utils.TargetInfo(results.download())
                report = Report(
                    extension=results_info.extension,
                    size=results_info.size,
                    checksum=results_info.checksum,
                    **report.model_dump(exclude={"extension", "size", "checksum"}),
                )

        if tracebacks:
            return Report(
                tracebacks=tracebacks,
                **report.model_dump(exclude={"tracebacks"}),
            )

        tracebacks = report.run_checks()
        return Report(
            tracebacks=tracebacks,
            **report.model_dump(exclude={"tracebacks"}),
        )

    @joblib.delayed  # type: ignore[misc]
    def _delayed_make_report(
        self, request: Request, invalidate_cache: bool, download: bool
    ) -> Report:
        with utils.tmp_working_dir():
            return self._make_report(
                request=request, invalidate_cache=invalidate_cache, download=download
            )

    def make_reports(
        self,
        requests: Sequence[Request] | None = None,
        reports_path: str | Path | None = None,
        invalidate_cache: bool = True,
        n_jobs: int = 1,
        verbose: int = 0,
        regex_pattern: str = "",
        download: bool = True,
    ) -> list[Report]:
        if reports_path and os.path.exists(reports_path):
            raise FileExistsError(reports_path)

        for licence in self.missing_licences:
            self.accept_licence(*licence)

        if requests is None:
            requests = [
                Request(collection_id=collection_id)
                for collection_id in self.collection_ids
            ]

        requests = [
            request
            for request in requests
            if re.search(regex_pattern, request.collection_id)
        ]

        if not download:
            requests = [_switch_off_download_checks(request) for request in requests]

        parallel = joblib.Parallel(n_jobs=n_jobs, verbose=verbose)
        reports: list[Report] = parallel(
            self._delayed_make_report(
                request=request,
                invalidate_cache=invalidate_cache,
                download=download,
            )
            for request in requests
        )
        if reports_path:
            with open(reports_path, "w") as fp:
                models.dump_reports(reports, fp)
        return reports
