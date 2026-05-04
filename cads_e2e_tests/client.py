import datetime
import functools
import logging
import time
from typing import Any

import attrs
import joblib
from ecmwf.datastores import Client, Collection, Collections, Remote

from . import utils
from .models import Report, Request

LOGGER = logging.getLogger(__name__)


SLEEP_INCREMENTAL_RATIO = 1.5


class CollectionUtils(utils.AbstractCollectionUtils):
    def __init__(self, collection: Collection) -> None:
        self.collection = collection

    @property
    def form(self) -> list[dict[str, Any]]:
        return self.collection.form

    def apply_constraints(self, parameters: dict[str, Any]) -> dict[str, Any]:
        return self.collection.apply_constraints(parameters)


def _licences_to_set_of_tuples(
    licences: list[dict[str, Any]],
) -> set[tuple[str, int]]:
    return {(licence["id"], licence["revision"]) for licence in licences}


def _get_elapsed_time(remote: Remote, max_replication_lag: float) -> float:
    assert max_replication_lag >= 0
    replication_lag = 0.0
    sleep = 1.0
    while True:
        if (started_at := remote.started_at) and (finished_at := remote.finished_at):
            return (finished_at - started_at).total_seconds()
        if replication_lag >= max_replication_lag:
            break
        sleep = min(sleep, max_replication_lag - replication_lag)
        time.sleep(sleep)
        replication_lag += sleep
        sleep *= SLEEP_INCREMENTAL_RATIO
    raise TimeoutError("Maximum replication lag exceeded.")


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

    def random_parameters(
        self, collection_id: str, parameters: dict[str, Any]
    ) -> dict[str, Any]:
        collection = self.get_collection(collection_id)
        collection_utils = CollectionUtils(collection)
        return collection_utils.random_parameters(parameters)

    def update_request_parameters(
        self,
        request: Request,
        cache_key: str | None,
    ) -> Request:
        parameters = dict(request.parameters)

        randomise = request.settings.randomise
        if randomise is None:
            randomise = not parameters

        if randomise:
            parameters = self.random_parameters(request.collection_id, parameters)

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
            sleep = min(sleep * SLEEP_INCREMENTAL_RATIO, self.sleep_max)

    def make_report(
        self,
        request: Request,
        cache_key: str | None,
        download: bool,
        max_runtime: float | None,
        max_replication_lag: float,
        get_elapsed_time: bool,
    ) -> Report:
        if request.settings.max_runtime is not None:
            max_runtime = request.settings.max_runtime

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

            elapsed_time = (
                _get_elapsed_time(remote, max_replication_lag)
                if get_elapsed_time
                else None
            )

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

    @joblib.delayed  # type: ignore[untyped-decorator]
    def delayed_make_report(
        self,
        request: Request,
        cache_key: str | None,
        download: bool,
        max_runtime: float | None,
        log_level: str | None,
        max_replication_lag: float,
        get_elapsed_time: bool,
        working_dir: str | None,
    ) -> Report:
        if log_level is not None:
            logging.basicConfig(level=log_level.upper())

        with utils.tmp_working_dir(working_dir):
            return self.make_report(
                request=request,
                cache_key=cache_key,
                download=download,
                max_runtime=max_runtime,
                max_replication_lag=max_replication_lag,
                get_elapsed_time=get_elapsed_time,
            )
