import random
from abc import ABC, abstractmethod
from typing import Any, Literal

from . import utils

WIDGETS_TYPES = Literal[
    "StringChoiceWidget",
    "StringListWidget",
    "GeographicLocationWidget",
    "FreeformInputWidget",
    "DateRangeWidget",
    "StringListArrayWidget",
    "GeographicExtentWidget",
]

MISSING_WIDGETS = {
    "ExclusiveFrameWidget",
    "ExclusiveGroupAccordionWidget",
    "ExclusiveGroupWidget",
    "FreeEditionWidget",
    "GeographicExtentMapWidget",
    "InclusiveGroupWidget",
    "LabelWidget",
}


class AbstractWidget(ABC):
    @abstractmethod
    def __init__(self, **details: Any):
        pass

    @property
    @abstractmethod
    def list_widget(self) -> bool:
        pass

    @abstractmethod
    def random_selection(self) -> Any:
        pass

    def normalise_selection(self, selection: Any) -> Any:
        if self.list_widget:
            return utils.ensure_list(selection)
        return selection


class StringChoiceWidget(AbstractWidget):
    def __init__(self, values: list[str], **details: Any):
        self.values = values

    @property
    def list_widget(self) -> bool:
        return False

    def random_selection(self) -> str:
        return random.choice(self.values)


class StringListWidget(AbstractWidget):
    def __init__(self, values: list[str], **details: Any):
        self.values = values

    @property
    def list_widget(self) -> bool:
        return True

    def random_selection(self) -> list[str]:
        return [random.choice(self.values)]


class GeographicLocationWidget(AbstractWidget):
    def __init__(
        self,
        minX: float = -180,
        minY: float = -90.0,
        maxX: float = 180,
        maxY: float = 90.0,
        stepX: float = 0.001,
        stepY: float = 0.001,
        **details: Any,
    ):
        self.min_x = minX
        self.min_y = minY
        self.max_x = maxX
        self.max_y = maxY
        self.step_x = stepX
        self.step_y = stepY

    @property
    def list_widget(self) -> bool:
        return False

    def random_selection(self) -> dict[str, float]:
        return {
            "latitude": utils.random_choice_from_range(
                self.min_y, self.max_y, self.step_y
            ),
            "longitude": utils.random_choice_from_range(
                self.min_x, self.max_x, self.step_x
            ),
        }


class FreeformInputWidget(AbstractWidget):
    def __init__(
        self,
        default: Any = None,
        dtype: str | None = None,
        **details: Any,
    ):
        self.default = default
        self.dtype = dtype

    @property
    def list_widget(self) -> bool:
        return False

    def random_selection(self) -> Any:
        if self.default is not None:
            return self.default

        match self.dtype:
            case "float":
                return 999.0
            case "int":
                return 999
            case "string":
                return ""

        return None


class DateRangeWidget(AbstractWidget):
    def __init__(self, minStart: str, maxEnd: str, **details: Any):
        self.min_start = minStart
        self.max_end = maxEnd

    @property
    def list_widget(self) -> bool:
        return True

    def random_selection(self) -> list[str]:
        return ["/".join([utils.random_date(self.min_start, self.max_end)] * 2)]


class StringListArrayWidget(AbstractWidget):
    def __init__(self, groups: list[dict[str, Any]], **details: Any):
        self.groups = groups

    @property
    def list_widget(self) -> bool:
        return True

    def random_selection(self) -> list[str]:
        values = []
        for group in self.groups:
            values.extend(group["values"])
        return [random.choice(values)]


class GeographicExtentWidget(AbstractWidget):
    def __init__(
        self,
        precision: int = 0,
        range: dict[str, float] = {"n": 90, "w": -360, "s": -90, "e": 360},
        maximum_extent: dict[str, float] = {"lat": 180, "lon": 360},
        **details: Any,
    ):
        step = 10 ** (-precision)
        step_x = details.get("stepX", step)
        step_y = details.get("stepY", step)

        self.step_x = step_x
        self.step_y = step_y
        self.range = range
        self.minimum_extent = details.get(
            "minimum_extent", {"lat": step_y, "lon": step_x}
        )
        self.maximum_extent = maximum_extent

    @property
    def list_widget(self) -> bool:
        return True

    def random_selection(self) -> list[float]:
        west, east = utils.random_range_from_range(
            self.range["w"],
            self.range["e"],
            self.step_x,
            self.minimum_extent["lon"],
            self.maximum_extent["lon"],
        )
        south, north = utils.random_range_from_range(
            self.range["s"],
            self.range["n"],
            self.step_y,
            self.minimum_extent["lat"],
            self.maximum_extent["lat"],
        )
        return [north, west, south, east]


def instantiate_widget(widget_type: WIDGETS_TYPES, **details: Any) -> AbstractWidget:
    match widget_type:
        case "StringChoiceWidget":
            return StringChoiceWidget(**details)
        case "StringListWidget":
            return StringListWidget(**details)
        case "GeographicLocationWidget":
            return GeographicLocationWidget(**details)
        case "FreeformInputWidget":
            return FreeformInputWidget(**details)
        case "DateRangeWidget":
            return DateRangeWidget(**details)
        case "StringListArrayWidget":
            return StringListArrayWidget(**details)
        case "GeographicExtentWidget":
            return GeographicExtentWidget(**details)
    raise NotImplementedError(f"{widget_type=}")
