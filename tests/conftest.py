from __future__ import annotations

import os

import pytest


@pytest.fixture()
def key() -> str:
    return os.getenv(
        "DATAPI_KEY",
        "00000000-0000-4000-a000-000000000000",
    )


@pytest.fixture()
def url() -> str:
    return os.getenv(
        "DATAPI_URL",
        "https://cds-stable-bopen.copernicus-climate.eu/api",
    )
