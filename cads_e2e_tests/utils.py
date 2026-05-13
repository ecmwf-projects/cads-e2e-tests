import contextlib
import dataclasses
import datetime
import hashlib
import logging
import os
import random
import tempfile
import traceback
from abc import ABC, abstractmethod
from typing import Any, Iterator, Literal, Type

DEFAULT_GEOGRAPHIC_LOCATION_DETAILS: dict[str, float] = {
    "minY": -90.0,
    "maxY": 90.0,
    "minX": -180.0,
    "maxX": 180.0,
    "stepY": 0.001,
    "stepX": 0.001,
}
DEFAULT_GEOGRAPHIC_EXTENT_DETAILS: dict[str, Any] = {
    "precision": 0,
    "range": {"n": 90, "w": -360, "s": -90, "e": 360},
    "maximum_extent": {"lat": 180, "lon": 360},
}

LIST_WIDGETS = [
    "DateRangeWidget",
    "StringListArrayWidget",
    "StringListWidget",
    "GeographicExtentWidget",
]


@contextlib.contextmanager
def tmp_working_dir(dir: str | None) -> Iterator[str]:
    old_dir = os.getcwd()
    with tempfile.TemporaryDirectory(dir=dir) as tmpdir:
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


def random_choice_from_range(start: float, stop: float, step: float = 1.0) -> float:
    return round(random.uniform(start, stop) / step) * step


def random_range_from_range(
    start: float,
    stop: float,
    step: float = 1.0,
    min_extent: float = 0,
    max_extent: float | None = None,
) -> tuple[float, float]:
    start = random_choice_from_range(start, stop - min_extent, step)
    if max_extent is not None:
        stop = min(stop, start + max_extent)
    stop = random_choice_from_range(start + min_extent, stop, step)
    return start, stop


def widget_random_selection(
    widget_type: Literal[
        "StringChoiceWidget",
        "StringListWidget",
        "GeographicLocationWidget",
        "FreeformInputWidget",
        "DateRangeWidget",
        "StringListArrayWidget",
        "GeographicExtentWidget",
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
        case "GeographicExtentWidget":
            details = DEFAULT_GEOGRAPHIC_EXTENT_DETAILS | details

            step = 10 ** (-details["precision"])
            details.setdefault("minimum_extent", {"lat": step, "lon": step})

            west, east = random_range_from_range(
                details["range"]["w"],
                details["range"]["e"],
                step,
                details["minimum_extent"]["lon"],
                details["maximum_extent"]["lon"],
            )
            south, north = random_range_from_range(
                details["range"]["s"],
                details["range"]["n"],
                step,
                details["minimum_extent"]["lat"],
                details["maximum_extent"]["lat"],
            )
            return [north, west, south, east]
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


class AbstractCollectionUtils(ABC):
    @property
    @abstractmethod
    def form(self) -> list[dict[str, Any]]:
        pass

    @abstractmethod
    def apply_constraints(self, parameters: dict[str, Any]) -> dict[str, Any]:
        pass

    def random_parameters(self, parameters: dict[str, Any]) -> dict[str, Any]:
        forms = {
            form["name"]: {k: v for k, v in form.items() if k != "name"}
            for form in self.form
            if form.get("name")
        }

        # Initialise parameters
        original_keys = list(parameters)
        parameters = {k: ensure_list(v) for k, v in parameters.items()}
        parameters = self.apply_constraints(parameters) | parameters
        added_keys = list(set(parameters) - set(original_keys))
        random.shuffle(added_keys)

        # Random selection based on constraints
        names = original_keys + added_keys
        for name in names:
            if value := parameters[name]:
                parameters[name] = random.choice(value)
            for k, v in self.apply_constraints(parameters).items():
                if names.index(k) > names.index(name) or v == []:
                    if k in original_keys:
                        v = list(set(v) & set(parameters[k]))
                    parameters[k] = v

        # Choose widgets to process
        for name, widget in forms.items():
            if widget["type"] == "GeographicExtentWidget" and not parameters.get(name):
                parameters.pop(name, None)
        widgets_to_add = {
            random.choice(form["children"])
            for form in forms.values()
            if form["type"] == "ExclusiveGroupAccordionWidget"
            and not set(form["children"]) & set(parameters)
        }

        widgets_to_skip = set(parameters)
        for name, widget in forms.items():
            if not widget.get("required") and name not in widgets_to_add:
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
                        parameters[name] = "/".join([random_date(start, end)] * 2)

        # Process widgets
        for name, widget in forms.items():
            if name in widgets_to_skip:
                continue
            parameters[name] = widget_random_selection(
                widget["type"], **widget.get("details", {})
            )

        return {
            name: ensure_list(value) if widget.get("type") in LIST_WIDGETS else value
            for name, widget in forms.items()
            if ensure_list(value := parameters.get(name))
        }
