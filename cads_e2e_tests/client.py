import datetime
import functools
import logging
import os
import random
import re
import time
from pathlib import Path
from typing import Any, Sequence

import attrs
import joblib
from cads_api_client import ApiClient
from cads_api_client.catalogue import Collections

from . import models, utils
from .models import Report, Request

LOGGER = logging.getLogger(__name__)


def _licences_to_set_of_tuples(
    licences: list[dict[str, Any]],
) -> set[tuple[str, int]]:
    return {(licence["id"], licence["revision"]) for licence in licences}


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

    def _make_report(self, request: Request, invalidate_cache: bool, dont_download: bool) -> Report:
        report = Report(request=request)

        tracebacks: list[str] = []
        with utils.catch_exceptions(tracebacks, logger=LOGGER):
            request = self.update_request_parameters(request, invalidate_cache)
            report = Report(
                request=request,
                **report.model_dump(exclude={"request"}),
            )

            remote = self.submit(request.collection_id, **request.parameters)
            request_uid = remote.request_uid
            LOGGER.debug(f"{request_uid=}")
            report = Report(
                request_uid=request_uid,
                **report.model_dump(exclude={"request_uid"}),
            )
            while remote.status == "accepted":
                time.sleep(1)

            tic = time.perf_counter()

            if dont_download:
                target = utils.RemoteTarget(remote.make_results().asset)
            else:
                target = utils.Target(remote.download())
        toc = time.perf_counter()

        if tracebacks:
            return Report(
                tracebacks=tracebacks,
                **report.model_dump(exclude={"tracebacks"}),
            )

        report = Report(
            time=toc - tic,
            extension=target.extension,
            size=target.size,
            checksum=target.checksum,
            **report.model_dump(exclude={"time", "extension", "size", "checksum"}),
        )
        tracebacks = report.run_checks()
        return Report(
            tracebacks=tracebacks,
            **report.model_dump(exclude={"tracebacks"}),
        )

    @joblib.delayed  # type: ignore[misc]
    def _delayed_make_report(self, request: Request, invalidate_cache: bool, dont_download: bool) -> Report:
        with utils.tmp_working_dir():
            return self._make_report(
                request=request, invalidate_cache=invalidate_cache,
                dont_download=dont_download,
            )

    def make_reports(
        self,
        requests: Sequence[Request] | None = None,
        reports_path: str | Path | None = None,
        invalidate_cache: bool = True,
        n_jobs: int = 1,
        verbose: int = 0,
        regex_pattern: str = "",
        dont_download: bool = False
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

        parallel = joblib.Parallel(n_jobs=n_jobs, verbose=verbose)
        reports: list[Report] = parallel(
            self._delayed_make_report(
                request=request, invalidate_cache=invalidate_cache,
                dont_download=dont_download
            )
            for request in requests
        )
        if reports_path:
            with open(reports_path, "w") as fp:
                models.dump_reports(reports, fp)
        return reports
