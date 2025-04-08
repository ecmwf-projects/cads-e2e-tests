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
from datapi import ApiClient, Remote
from datapi.catalogue import Collections

from . import models, utils
from .models import Checks, Report, Request

LOGGER = logging.getLogger(__name__)
DOWNLOAD_CHECKS = {"checksum", "extension", "size"}
LIST_WIDGETS = [
    "DateRangeWidget",
    "StringListArrayWidget",
    "StringListWidget",
]


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


def _ensure_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if value is None:
        return []
    if isinstance(value, tuple | set | range):
        return list(value)
    return [value]


@attrs.define
class TestClient(ApiClient):
    __test__ = False

    @functools.cached_property
    def missing_licences(self) -> set[tuple[str, int]]:
        if self.check_authentication().get("role") == "anonymous":
            return set()

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
        return [
            collection_id
            for collection_id in collection_ids
            if self.get_collection(collection_id).json.get("cads:disabled_reason")
            is None
        ]

    def random_parameters(self, collection_id: str) -> dict[str, Any]:
        collection = self.get_collection(collection_id)
        forms = {
            name: form for form in collection.form if (name := form.pop("name", None))
        }

        # Random selection based on constraints
        parameters = collection.apply_constraints({})
        names = list(parameters)
        random.shuffle(names)
        for name in names:
            if value := parameters[name]:
                parameters[name] = random.choice(value)
            for k, v in collection.apply_constraints(parameters).items():
                if names.index(k) > names.index(name) or v == []:
                    parameters[k] = v

        # Choose widgets to process
        widgets_to_skip = set(parameters)
        for name, widget in forms.items():
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
        for name, widget in forms.items():
            if name in widgets_to_skip:
                continue

            match widget["type"]:
                case "StringChoiceWidget" | "StringListWidget":
                    parameters[name] = random.choice(widget["details"]["values"])
                case "GeographicLocationWidget":
                    location = {}
                    for coord in ("latitude", "longitude"):
                        details = widget.get("details", {}).get(coord, {})
                        match coord:
                            case "latitude":
                                coord_range = {"min": -90, "max": 90}
                            case "longitude":
                                coord_range = {"min": -180, "max": 180}
                            case _:
                                coord_range = {}
                        coord_range |= details.get("range", {})
                        location[coord] = round(
                            random.uniform(coord_range["min"], coord_range["max"]),
                            details.get("precision", 1),
                        )
                    parameters[name] = location
                case "FreeformInputWidget":
                    if (value := widget["details"].get("default")) is None:
                        match widget["details"].get("dtype"):
                            case "string":
                                value = ""
                            case "float":
                                value = -999.0
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

        return {
            name: _ensure_list(value) if widget.get("type") in LIST_WIDGETS else value
            for name, widget in forms.items()
            if _ensure_list(value := parameters.get(name))
        }

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

    def _wait_on_results_with_timeout(
        self, remote: Remote, max_runtime: float | None
    ) -> None:
        sleep = 1.0
        while not remote.results_ready:
            if (
                max_runtime is not None
                and remote.finished_at is None
                and (started_at := remote.started_at) is not None
            ):
                if started_at.tzinfo is None:
                    started_at = started_at.replace(tzinfo=datetime.timezone.utc)
                timedelta = datetime.datetime.now(datetime.timezone.utc) - started_at
                if timedelta.total_seconds() > max_runtime:
                    raise TimeoutError("Maximum runtime exceeded.")
            time.sleep(sleep)
            sleep = min(sleep * 1.5, self.sleep_max)

    def _make_report(
        self,
        request: Request,
        cache_key: str | None,
        download: bool,
        max_runtime: float | None,
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
                request_uid=remote.request_id,
                **report.model_dump(exclude={"request_uid"}),
            )

            self._wait_on_results_with_timeout(remote, max_runtime)
            results = remote.get_results()

            time.sleep(1)  # Make sure start/end datetimes are updated
            assert remote.started_at is not None and remote.finished_at is not None
            elapsed_time = (remote.finished_at - remote.started_at).total_seconds()

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

        if not tracebacks:
            tracebacks = report.run_checks()

        return Report(
            tracebacks=tracebacks,
            **report.model_dump(exclude={"tracebacks", "finished_at"}),
        )

    @joblib.delayed  # type: ignore[misc]
    def _delayed_make_report(
        self,
        request: Request,
        cache_key: str | None,
        download: bool,
        max_runtime: float | None,
        log_level: str | None,
    ) -> Report:
        if log_level is not None:
            logging.basicConfig(level=log_level.upper())
        with utils.tmp_working_dir():
            return self._make_report(
                request=request,
                cache_key=cache_key,
                download=download,
                max_runtime=max_runtime,
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
        cyclic: bool = True,
        randomise: bool = False,
        max_runtime: float | None = None,
        log_level: str | None = None,
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

        requests = utils.reorder(
            requests,
            cyclic=cyclic,
            randomise=randomise,
            n_repeats=n_repeats,
        )

        parallel = joblib.Parallel(n_jobs=n_jobs, verbose=verbose)
        reports: list[Report] = parallel(
            self._delayed_make_report(
                request=request,
                cache_key=cache_key,
                download=download,
                max_runtime=max_runtime,
                log_level=log_level,
            )
            for request in requests
        )
        if reports_path:
            with open(reports_path, "w") as fp:
                models.dump_reports(reports, fp)
        return reports
