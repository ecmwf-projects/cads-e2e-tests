import contextlib
import dataclasses
import datetime
import hashlib
import logging
import os
import random
import tempfile
import traceback
from typing import Any, Iterator, Literal, Type

DEFAULT_GEOGRAPHIC_LOCATION_DETAILS: dict[str, float] = {
    "minY": -90.0,
    "maxY": 90.0,
    "minX": -180.0,
    "maxX": 180.0,
    "stepY": 0.001,
    "stepX": 0.001,
}


@contextlib.contextmanager
def tmp_working_dir() -> Iterator[str]:
    old_dir = os.getcwd()
    with tempfile.TemporaryDirectory() as tmpdir:
        os.chdir(tmpdir)
        try:
            yield tmpdir
        finally:
            os.chdir(old_dir)


@contextlib.contextmanager
def catch_exceptions(
    tracebacks: list[str],
    exceptions: tuple[Type[Exception]] = (Exception,),
    logger: logging.Logger | None = None,
) -> Iterator[None]:
    try:
        yield
    except exceptions:
        if logger:
            logger.exception("")
        tracebacks.append(traceback.format_exc())


@dataclasses.dataclass
class TargetInfo:
    target: str

    @property
    def checksum(self) -> str:
        with open(self.target, "rb") as f:
            digest = hashlib.file_digest(f, "md5")
        return digest.hexdigest()

    @property
    def extension(self) -> str:
        _, extension = os.path.splitext(self.target)
        return extension

    @property
    def size(self) -> int:
        return os.path.getsize(self.target)

    @property
    def inspect(self) -> int:
        return os.system(f"$INSPECT_E2ETESTS_RESULT {self.target}")

def random_date(start: str, end: str) -> str:
    start_date = datetime.date.fromisoformat(start)
    end_date = datetime.date.fromisoformat(end)
    days = random.randint(0, (end_date - start_date).days)
    random_date = start_date + datetime.timedelta(days=days)
    return random_date.isoformat()


def reorder(
    requests: list[Any],
    cyclic: bool,
    randomise: bool,
    n_repeats: int,
) -> list[Any]:
    if cyclic:
        output = []
        for _ in range(n_repeats):
            output.extend(
                random.sample(requests, len(requests)) if randomise else requests
            )
        return output

    return [
        request
        for request in (
            random.sample(requests, len(requests)) if randomise else requests
        )
        for _ in range(n_repeats)
    ]


def random_choice_from_range(min: float, max: float, step: float = 1.0) -> float:
    return round(random.uniform(min, max) / step) * step


def widget_random_selection(
    widget_type: Literal[
        "StringChoiceWidget",
        "StringListWidget",
        "GeographicLocationWidget",
        "FreeformInputWidget",
        "DateRangeWidget",
        "StringListArrayWidget",
    ],
    **details: Any,
) -> Any:
    match widget_type:
        case "StringChoiceWidget" | "StringListWidget":
            return random.choice(details["values"])
        case "GeographicLocationWidget":
            details = DEFAULT_GEOGRAPHIC_LOCATION_DETAILS | details
            return {
                coord: random_choice_from_range(
                    *[details[f"{prefix}{suffix}"] for prefix in ("min", "max", "step")]
                )
                for coord, suffix in zip(["latitude", "longitude"], ["Y", "X"])
            }
        case "FreeformInputWidget":
            if (value := details.get("default")) is None:
                match details.get("dtype"):
                    case "float":
                        value = 999.0
                    case "int":
                        value = 999
                    case "string":
                        value = ""
            return value
        case "DateRangeWidget":
            return "/".join([random_date(details["minStart"], details["maxEnd"])] * 2)
        case "StringListArrayWidget":
            values = []
            for group in details["groups"]:
                values.extend(group["values"])
            return random.choice(values)
        case _:
            raise NotImplementedError(f"{widget_type=}")


def ensure_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if value is None:
        return []
    if isinstance(value, tuple | set | range):
        return list(value)
    return [value]
