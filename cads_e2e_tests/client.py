import datetime
import json
import logging
import random
from pathlib import Path
from typing import Any, Sequence

import attrs
import tqdm
from cads_api_client import ApiClient
from cads_api_client.catalogue import Collections

from . import exceptions, utils

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

    def make_single_report(self, cache: bool, **request: Any) -> dict[str, Any]:
        collection_id = request.pop("collection_id")
        parameters = request.pop("parameters", {})
        checks = request.pop("checks", {})
        assert not request

        if not parameters:
            parameters = self.random_parameters(collection_id)
        if not cache:
            parameters.setdefault("_timestamp", datetime.datetime.now().isoformat())
        report: dict[str, Any] = {
            "collection_id": collection_id,
            "parameters": parameters,
            "checks": checks,
            "tracebacks": [],
        }

        with utils.catch_exception(
            report,
            LOGGER,
            elapsed_time=True,
            allowed_exceptions=(Exception,),
        ):
            remote = self.collection(collection_id).submit(**parameters)
            report["request_uid"] = remote.request_uid

            target = remote.download(retry_options=RETRY_OPTIONS)
        report.update(utils.get_target_info(target))

        if not report["tracebacks"]:
            with utils.catch_exception(
                report,
                LOGGER,
                elapsed_time=False,
                allowed_exceptions=(exceptions.CheckError,),
            ):
                report = utils.check_report(report, **checks)

        return report

    def make_report(
        self,
        requests: Sequence[dict[str, Any]] | None = None,
        report_path: str | Path | None = None,
        cache: bool = False,
    ) -> list[dict[str, Any]]:
        if requests is None:
            # One random request per dataset
            requests = [
                {"collection_id": collection_id} for collection_id in self.collecion_ids
            ]

        for request in requests:
            try:
                utils.validate_request(request)
            except AssertionError as exc:
                raise ValueError(f"Invalid request: {request}") from exc

        reports = []
        for request in tqdm.tqdm(requests):
            with utils.tmp_working_dir():
                report = self.make_single_report(cache=cache, **request)
            reports.append(report)

            if report_path is not None:
                with open(report_path, "w") as fp:
                    json.dump(reports, fp)
        return reports
