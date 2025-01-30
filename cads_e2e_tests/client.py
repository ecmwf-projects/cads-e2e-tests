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
from datapi import ApiClient
from datapi.catalogue import Collections

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
    return Request(checks=checks, **request.model_dump(exclude={"checks"}))


@attrs.define
class TestClient(ApiClient):
    __test__ = False

    @functools.cached_property
    def missing_licences(self) -> set[tuple[str, int]]:
        licences = _licences_to_set_of_tuples(self.get_licences())
        accepted_licences = _licences_to_set_of_tuples(self.get_accepted_licences())
        return licences - accepted_licences

    @functools.cached_property
    def collection_ids(self) -> list[str]:
        collection_ids = []
        collections: Collections | None = self.get_collections()
        while collections is not None:
            collection_ids.extend(collections.collection_ids)
            collections = collections.next
        return collection_ids

    def random_parameters(self, collection_id: str) -> dict[str, Any]:
        collection = self.get_collection(collection_id)

        # Random selection based on constraints
        parameters = collection.process.apply_constraints({})
        for key in sorted(parameters):
            if value := parameters[key]:
                parameters[key] = random.choice(value)
            for k, v in collection.process.apply_constraints(parameters).items():
                if k > key or v == []:
                    parameters[k] = v

        # Choose widgets to process
        widgets_to_skip = set(parameters)
        for widget in collection.form:
            name = widget["name"]
            if not widget.get("required"):
                widgets_to_skip.add(name)

            match widget["type"]:
                case "ExclusiveFrameWidget" | "InclusiveFrameWidget":
                    widgets_to_skip.add(name)
                    widgets_to_skip.update(widget["widgets"])
                case "DateRangeWidget":
                    if date := parameters.get(name):
                        # Select one day
                        start = date.split("/")[0]
                        end = date.split("/")[-1]
                        parameters[name] = "/".join([utils.random_date(start, end)] * 2)

        # Process widgets
        for widget in collection.form:
            name = widget["name"]
            if name in widgets_to_skip:
                continue

            match widget["type"]:
                case "StringChoiceWidget" | "StringListWidget":
                    parameters[name] = random.choice(widget["details"]["values"])
                case "GeographicLocationWidget":
                    location = {}
                    for coord, details in widget["details"].items():
                        location[coord] = round(
                            random.uniform(*details["range"].values()),
                            details["precision"],
                        )
                    parameters[name] = location
                case "FreeformInputWidget":
                    value = widget["details"]["default"]
                    if isinstance(value, list):
                        value = random.choice(value)
                    parameters[name] = value
                case "DateRangeWidget":
                    start = widget["details"]["minStart"]
                    end = widget["details"]["maxEnd"]
                    parameters[name] = "/".join([utils.random_date(start, end)] * 2)
                case "StringListArrayWidget":
                    values = []
                    for group in widget["details"]["groups"]:
                        values.extend(group["values"])
                    parameters[name] = random.choice(values)
                case widget_type:
                    raise NotImplementedError(f"{widget_type=}")

        return {k: v for k, v in parameters.items() if v != []}

    def update_request_parameters(
        self,
        request: Request,
        cache_key: str | None,
    ) -> Request:
        parameters = dict(request.parameters)
        if not parameters:
            parameters = self.random_parameters(request.collection_id)
        if cache_key is not None:
            parameters.setdefault(cache_key, datetime.datetime.now().isoformat())
        return Request(
            parameters=parameters,
            **request.model_dump(exclude={"parameters"}),
        )

    def _make_report(
        self, request: Request, cache_key: str | None, download: bool
    ) -> Report:
        report = Report(request=request)

        tracebacks: list[str] = []
        with utils.catch_exceptions(tracebacks, logger=LOGGER):
            request = self.update_request_parameters(request, cache_key)
            report = Report(
                request=request,
                **report.model_dump(exclude={"request"}),
            )

            remote = self.submit(request.collection_id, request.parameters)
            report = Report(
                request_uid=remote.request_uid,
                **report.model_dump(exclude={"request_uid"}),
            )

            results = remote.make_results()
            time.sleep(1)  # Make sure start/end datetimes are updated
            assert remote.start_datetime is not None and remote.end_datetime is not None
            elapsed_time = (remote.end_datetime - remote.start_datetime).total_seconds()

            report = Report(
                time=elapsed_time,
                content_length=results.content_length,
                content_type=results.content_type,
                **report.model_dump(exclude={"time", "content_length", "content_type"}),
            )
            if download:
                target_info = utils.TargetInfo(results.download())
                report = Report(
                    extension=target_info.extension,
                    size=target_info.size,
                    checksum=target_info.checksum,
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
        self, request: Request, cache_key: str | None, download: bool
    ) -> Report:
        with utils.tmp_working_dir():
            return self._make_report(
                request=request, cache_key=cache_key, download=download
            )

    def make_reports(
        self,
        requests: Sequence[Request] | None = None,
        reports_path: str | Path | None = None,
        cache_key: str | None = None,
        n_jobs: int = 1,
        verbose: int = 0,
        regex_pattern: str = "",
        download: bool = True,
        n_repeats: int = 1,
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
                cache_key=cache_key,
                download=download,
            )
            for request in requests * n_repeats
        )
        if reports_path:
            with open(reports_path, "w") as fp:
                models.dump_reports(reports, fp)
        return reports
