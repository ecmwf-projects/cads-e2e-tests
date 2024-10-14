from typing import Any


class CheckError(Exception):
    def __init__(self, actual: Any, expected: Any) -> None:
        self.actual = actual
        self.expected = expected
        super().__init__(self._message)

    @property
    def _message(self) -> str:
        return f"actual={self.actual!r} expected={self.expected!r}"

    def __str__(self) -> str:
        return self._message


class ExtensionError(CheckError):
    pass


class SizeError(CheckError):
    pass


class TimeError(CheckError):
    pass


class ChecksumError(CheckError):
    pass


class ContentLengthError(CheckError):
    pass


class ContentTypeError(CheckError):
    pass
