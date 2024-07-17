import datetime
import json
import logging
import os
import random
from typing import Any, Sequence

import attrs
import tqdm
from cads_api_client import ApiClient
from cads_api_client.catalogue import Collections

from . import utils

LOGGER = logging.getLogger(__name__)
RETRY_OPTIONS = {"maximum_tries": 0}


def _licences_to_set_of_tuples(
    licences: dict[str, Any],
) -> set[tuple[str, int]]:
    return {(licence["id"], licence["revision"]) for licence in licences["licences"]}


@attrs.define
class TestClient(ApiClient):
    def __attrs_post_init__(self) -> None:
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

    def random_request(self, collection_id: str) -> dict[str, Any]:
        request = self.valid_values(collection_id, {})
        for key in list(request):
            if values := self.valid_values(collection_id, request)[key]:
                request[key] = random.choice(values)
            else:
                request.pop(key)
        return request

    def get_download_report(
        self,
        collection_id: str,
        expected_ext: str | None = None,
        expected_size: int | None = None,
        expected_time: float | None = None,
        **request: Any,
    ) -> dict[str, Any]:
        # Normalise request
        if not request:
            request = self.random_request(collection_id)
        request.setdefault("_timestamp", datetime.datetime.now().isoformat())
        report: dict[str, Any] = {
            "collection_id": collection_id,
            "request": request,
            "tracebacks": [],
        }

        with utils.catch_exception(
            report, LOGGER, elapsed_time=True, exceptions=(Exception,)
        ):
            remote = self.collection(collection_id).submit(**request)
            report["request_uid"] = remote.request_uid

            target = remote.download(retry_options=RETRY_OPTIONS)
            report["target"] = target
            report["size"] = os.path.getsize(target)

        if not report["tracebacks"]:
            with utils.catch_exception(
                report, LOGGER, elapsed_time=False, exceptions=(AssertionError,)
            ):
                report = utils.check_report(
                    report,
                    expected_ext=expected_ext,
                    expected_size=expected_size,
                    expected_time=expected_time,
                )
        return report

    def make_report(
        self,
        requests: Sequence[dict[str, Any]] | None = None,
        report_path: str | None = None,
    ) -> list[dict[str, Any]]:
        if requests is None:
            # One random request per dataset
            requests = [
                {"collection_id": collection_id} for collection_id in self.collecion_ids
            ]

        reports = []
        for request in tqdm.tqdm(requests):
            with utils.tmp_working_dir():
                report = self.get_download_report(
                    collection_id=request.pop("collection_id"),
                    expected_size=request.pop("expected_size", None),
                    expected_ext=request.pop("expected_ext", None),
                    expected_time=request.pop("expected_time", None),
                    **request,
                )
            reports.append(report)

            if report_path is not None:
                with open(report_path, "w") as fp:
                    json.dump(reports, fp)
        return reports
