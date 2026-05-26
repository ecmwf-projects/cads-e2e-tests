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
from typing import Any, Iterator, Type

from . import widgets

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
    assert min_extent >= 0
    assert (stop - start) >= min_extent
    start = random_choice_from_range(start, stop - min_extent, step)
    if max_extent is not None:
        stop = min(stop, start + max_extent)
    stop = random_choice_from_range(start + min_extent, stop, step)
    return start, stop


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
        for name, form in forms.items():
            if form["type"] == "GeographicExtentWidget" and not parameters.get(name):
                parameters.pop(name, None)
        widgets_to_add = {
            random.choice(form["children"])
            for form in forms.values()
            if form["type"] == "ExclusiveGroupAccordionWidget"
            and not set(form["children"]) & set(parameters)
        }

        widgets_to_skip = set(parameters)
        for name, form in forms.items():
            if not form.get("required") and name not in widgets_to_add:
                widgets_to_skip.add(name)

            match form["type"]:
                case "ExclusiveFrameWidget" | "InclusiveFrameWidget":
                    widgets_to_skip.add(name)
                    widgets_to_skip.update(form["widgets"])
                case "DateRangeWidget":
                    if date := parameters.get(name):
                        # Select one day
                        start = date.split("/")[0]
                        end = date.split("/")[-1]
                        parameters[name] = "/".join([random_date(start, end)] * 2)

        # Process widgets
        normalised_parameters = {}
        for name, form in forms.items():
            widget = widgets.instantiate_widget(form["type"], **form.get("details", {}))
            if name in widgets_to_skip:
                value = parameters.get(name)
            else:
                value = widget.random_selection()
            if ensure_list(value):
                normalised_parameters[name] = widget.normalise_selection(value)
        return normalised_parameters
