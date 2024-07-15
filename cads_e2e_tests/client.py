import contextlib
import datetime
import json
import os
import random
import tempfile
import time
from typing import Any, Iterator

import attrs
import tqdm
from cads_api_client import ApiClient
from cads_api_client.catalogue import Collections


def _licences_to_set_of_tuples(
    licences: dict[str, Any],
) -> set[tuple[str, int]]:
    return {(licence["id"], licence["revision"]) for licence in licences["licences"]}


@contextlib.contextmanager
def tmp_working_dir() -> Iterator[str]:
    old_dir = os.getcwd()
    with tempfile.TemporaryDirectory() as tmpdir:
        os.chdir(tmpdir)
        try:
            yield tmpdir
        finally:
            os.chdir(old_dir)


@attrs.define
class Client(ApiClient):
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

    def test_random_request(self, collection_id: str) -> dict[str, Any]:
        request = self.random_request(collection_id)
        request["_no_cache"] = datetime.datetime.now().isoformat()
        collection = self.collection(collection_id)

        tic = time.perf_counter()
        remote = collection.submit(**request)
        report: dict[str, Any] = {
            "request": request,
            "request_uid": remote.request_uid,
        }
        with tmp_working_dir():
            try:
                report["target"] = remote.download()
            except Exception as exc:
                report["exception"] = str(exc)
            else:
                toc = time.perf_counter()
                report["elapsed_time"] = toc - tic

        return report

    def test_random_requests(self, report_path: str) -> dict[str, dict[str, Any]]:
        report = {}
        for collection_id in tqdm.tqdm(self.collecion_ids):
            report[collection_id] = self.test_random_request(collection_id)
            with open(report_path, "w") as fp:
                json.dump(report, fp)
        return report
