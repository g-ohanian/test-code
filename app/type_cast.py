from datetime import datetime

from rest_framework import status
from rest_framework.exceptions import APIException


class UnsupportedFilterType(APIException):
    status_code = status.HTTP_422_UNPROCESSABLE_ENTITY

    def __init__(self, detail: str, code: int = status_code) -> None:
        super().__init__(detail=detail, code=code)


class Cast:
    def __init__(self, value):
        self._value = value

    def to_bool(self):
        value_ranges = ("true", "false", "1", "0")
        if isinstance(self._value, bool):
            return self._value
        value = str(self._value).lower()
        if value not in value_ranges:
            raise UnsupportedFilterType("Value is not boolean")
        return value in ("true", "1")

    def to_date_time(self, date_format) -> datetime:
        try:
            return datetime.strptime(self._value, date_format)
        except ValueError:
            raise UnsupportedFilterType("Value is not a valid date or has not appropriate format.")

    def to_int(self):
        try:
            return int(self._value)
        except ValueError:
            raise UnsupportedFilterType("Value is not a valid integer")
