from __future__ import annotations

import os
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from cads_e2e_tests.client import TestClient


@pytest.fixture()
def key() -> str:
    return os.getenv(
        "CADS_API_KEY",
        "00000000-0000-4000-a000-000000000000",
    )


@pytest.fixture()
def url() -> str:
    return os.getenv(
        "CADS_API_URL",
        "https://cds-dev-bopen.copernicus-climate.eu/api",
    )


@pytest.fixture()
def client(key: str, url: str) -> TestClient:
    from cads_e2e_tests.client import TestClient

    return TestClient(key=key, url=url)
