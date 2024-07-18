class CheckError(Exception):
    pass


class ExtensionError(CheckError):
    pass


class SizeError(CheckError):
    pass


class TimeError(CheckError):
    pass


class ChecksumError(CheckError):
    pass
