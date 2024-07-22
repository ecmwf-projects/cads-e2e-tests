import datetime
import logging
import random
import time
from pathlib import Path
from typing import Any, Sequence

import attrs
import tqdm
from cads_api_client import ApiClient
from cads_api_client.catalogue import Collections

from . import models, utils
from .models import Report, Request

LOGGER = logging.getLogger(__name__)
RETRY_OPTIONS = {"maximum_tries": 0}


def _licences_to_set_of_tuples(
    licences: dict[str, Any],
) -> set[tuple[str, int]]:
    return {(licence["id"], licence["revision"]) for licence in licences["licences"]}


@attrs.define
class TestClient(ApiClient):
    def __attrs_post_init__(self) -> None:
        # TODO: Remove when anonymous user is implemented
        for licence in self.missing_licences:
            self.accept_licence(*licence)

    @property
    def missing_licences(self) -> set[tuple[str, int]]:
        licences = _licences_to_set_of_tuples(self.licences)
        accepted_licences = _licences_to_set_of_tuples(self.accepted_licences)
        return licences - accepted_licences

    @property
    def collecion_ids(self) -> list[str]:
        collection_ids = []
        collections: Collections | None = self.collections()
        while collections is not None:
            collection_ids.extend(collections.collection_ids())
            collections = collections.next()
        return collection_ids

    def random_parameters(self, collection_id: str) -> dict[str, Any]:
        parameters = self.valid_values(collection_id, {})
        for key in list(parameters):
            if choices := self.valid_values(collection_id, parameters)[key]:
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

    def _make_report(self, request: Request, invalidate_cache: bool) -> Report:
        request = self.update_request_parameters(request, invalidate_cache)
        report = Report(request=request)

        tic = time.perf_counter()
        with utils.catch_exceptions(report.tracebacks, logger=LOGGER):
            remote = self.collection(request.collection_id).submit(**request.parameters)
            report = Report(
                request_uid=remote.request_uid,
                **report.model_dump(exclude={"request_uid"}),
            )
            target = utils.Target(remote.download(retry_options=RETRY_OPTIONS))
        toc = time.perf_counter()

        if report.tracebacks:
            return report

        report = Report(
            time=toc - tic,
            extension=target.extension,
            size=target.size,
            checksum=target.checksum,
            **report.model_dump(exclude={"time", "extension", "size", "checksum"}),
        )
        report.run_checks()
        return report

    def make_reports(
        self,
        requests: Sequence[Request] | None = None,
        reports_path: str | Path | None = None,
        invalidate_cache: bool = True,
    ) -> list[Report]:
        if requests is None:
            # One random request per dataset
            requests = [
                Request(collection_id=collection_id)
                for collection_id in self.collecion_ids
            ]

        reports = []
        for request in tqdm.tqdm(requests):
            with utils.tmp_working_dir():
                report = self._make_report(
                    request=request, invalidate_cache=invalidate_cache
                )
            reports.append(report)

            if reports_path is not None:
                with open(reports_path, "w") as fp:
                    models.dump_reports(reports, fp)
        return reports
