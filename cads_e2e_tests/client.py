import datetime
import functools
import logging
import random
import time
from typing import Any

import attrs
import joblib
from ecmwf.datastores import Client, Collections, Remote

from . import utils
from .models import Report, Request

LOGGER = logging.getLogger(__name__)

LIST_WIDGETS = [
    "DateRangeWidget",
    "StringListArrayWidget",
    "StringListWidget",
]


def _licences_to_set_of_tuples(
    licences: list[dict[str, Any]],
) -> set[tuple[str, int]]:
    return {(licence["id"], licence["revision"]) for licence in licences}


def _ensure_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if value is None:
        return []
    if isinstance(value, tuple | set | range):
        return list(value)
    return [value]


@attrs.define
class TestClient(Client):
    __test__ = False

    @functools.cached_property
    def missing_licences(self) -> set[tuple[str, int]]:
        if self.check_authentication().get("role") == "anonymous":
            return set()

        licences = _licences_to_set_of_tuples(self.get_licences())
        accepted_licences = _licences_to_set_of_tuples(self.get_accepted_licences())
        return licences - accepted_licences

    def accept_all_missing_licences(self) -> None:
        for licence in self.missing_licences:
            self.accept_licence(*licence)

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
            parameters[name] = utils.widget_random_selection(
                widget["type"], **widget.get("details", {})
            )

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

    def wait_on_results_with_timeout(
        self, remote: Remote, max_runtime: float | None
    ) -> None:
        sleep = 1.0
        while not remote.results_ready:
            if (
                max_runtime is not None
                and (started_at := remote.started_at) is not None
            ):
                if started_at.tzinfo is None:
                    started_at = started_at.replace(tzinfo=datetime.timezone.utc)
                timedelta = datetime.datetime.now(datetime.timezone.utc) - started_at
                if timedelta.total_seconds() > max_runtime:
                    raise TimeoutError("Maximum runtime exceeded.")
            time.sleep(sleep)
            sleep = min(sleep * 1.5, self.sleep_max)

    def make_report(
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

            self.wait_on_results_with_timeout(remote, max_runtime)
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
    def delayed_make_report(
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
            return self.make_report(
                request=request,
                cache_key=cache_key,
                download=download,
                max_runtime=max_runtime,
            )
