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
import yaml
from cads_api_client import ApiClient
from cads_api_client.catalogue import Collections


def _licences_to_set_of_tuples(
    licences: dict[str, Any],
) -> set[tuple[str, int]]:
    return {(licence["id"], licence["revision"]) for licence in licences["licences"]}


def check_report(
    report: dict[str, Any],
    expected_ext: str | None,
    expected_size: int | None,
) -> dict[str, Any]:
    if "exception" in report:
        return report

    target = report["target"]
    try:
        if expected_ext is not None:
            _, ext = os.path.splitext(target)
            assert ext == expected_ext, f"{ext=} {expected_ext=}"

        if expected_size is not None:
            size = os.path.getsize(target)
            assert size == expected_size, f"{size=} {expected_size=}"
    except AssertionError as exc:
        report["exception"] = str(exc)
    return report


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

    def get_download_report(
        self,
        collection_id: str,
        expected_ext: str | None = None,
        expected_size: int | None = None,
        **request: Any,
    ) -> dict[str, Any]:
        # Normalise request
        if not request:
            request = self.random_request(collection_id)
        assert "target" not in request
        request.setdefault("_timestamp", datetime.datetime.now().isoformat())
        request.setdefault("retry_options", {})
        request["retry_options"].setdefault("maximum_tries", 0)

        collection = self.collection(collection_id)
        report: dict[str, Any] = {"collection_id": collection_id, "request": request}
        tic = time.perf_counter()
        try:
            remote = collection.submit(**request)
            report["request_uid"] = remote.request_uid
            report["target"] = remote.download()
        except Exception as exc:
            report["exception"] = str(exc)
        else:
            toc = time.perf_counter()
            report["elapsed_time"] = toc - tic

        return check_report(
            report,
            expected_ext=expected_ext,
            expected_size=expected_size,
        )

    def write_report(
        self,
        report_path: str,
        requests_yaml_path: str | None = None,
    ) -> list[dict[str, Any]]:
        if requests_yaml_path is None:
            # Random requests
            requests: list[dict[str, Any]] = [
                {"collection_id": collection_id} for collection_id in self.collecion_ids
            ]
        else:
            with open(requests_yaml_path, "r") as fp:
                requests = yaml.safe_load(fp)

        reports = []
        for request in tqdm.tqdm(requests):
            with tmp_working_dir():
                report = self.get_download_report(
                    collection_id=request.pop("collection_id"),
                    expected_size=request.pop("expected_size", None),
                    expected_ext=request.pop("expected_ext", None),
                    **request,
                )
            reports.append(report)

            with open(report_path, "w") as fp:
                json.dump(reports, fp)
        return reports
