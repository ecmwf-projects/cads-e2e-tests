from __future__ import annotations

import os
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from cads_e2e_tests.client import TestClient


@pytest.fixture()
def client() -> TestClient:
    from cads_e2e_tests.client import TestClient

    return TestClient(
        url=os.getenv(
            "CADS_API_URL",
            "https://cds-stable-bopen.copernicus-climate.eu/api",
        ),
        key=os.getenv(
            "CADS_API_KEY",
            "00000000-0000-4000-a000-000000000000",
        ),
    )
