from abc import ABC, abstractmethod
from typing import Any, Self, Iterable

from django.core.exceptions import FieldDoesNotExist
from django.db.models import DateTimeField, DateField
from django.db.models import QuerySet, Q
from django.db.models.functions import Cast as DjangoCast, Lower

from type_cast import UnsupportedFilterType, Cast


class AbstractFilter(ABC):
    """Custom Filtering. Designed to be used with the React Grid Table fields"""
    ANNOTATION_NAME = ""
    OPERATORS_MAP = {}

    def __init__(self, queryset) -> None:
        self._queryset = queryset

    @abstractmethod
    def _cast(self, value) -> Any:
        pass

    @property
    def queryset(self) -> QuerySet:
        return self._queryset

    def _normalize_value(self, value):
        return value

    def _map_operator(self, operator) -> str:
        if hasattr(self, f"_{operator}"):
            return operator
        return self.OPERATORS_MAP.get(operator, "eq")

    def _resolve_field_name(self, field):
        field = self.ANNOTATION_NAME or field
        return field

    def query_filter(self, field, value, operator="eq", **kwargs) -> Self:
        value = self._cast(value) if operator not in ("isEmpty", "isNotEmpty") else value
        value = self._normalize_value(value)
        operator = self._map_operator(operator)
        return getattr(self, f"_{operator}")(self._resolve_field_name(field), value, **kwargs)

    def _eq(self, field, value, **kwargs) -> Self:
        self._queryset = self._queryset.filter(**{field: value})
        return self

    def _not(self, field, value, **kwargs) -> Self:
        self._queryset = self._queryset.exclude(**{field: value})
        return self

    def _in(self, field, value, **kwargs) -> Self:
        if not isinstance(value, (list, tuple)):
            value = (value,)
        self._queryset = self._queryset.filter(**{f"{field}__in": value})
        return self

    def _is_grater_than(self, field, value, **kwargs) -> Self:
        self._queryset = self._queryset.filter(**{f"{field}__gt": value})
        return self

    def _is_grater_than_or_equal(self, field, value, **kwargs) -> Self:
        self._queryset = self._queryset.filter(**{f"{field}__gte": value})
        return self

    def _is_less_than(self, field, value, **kwargs) -> Self:
        self._queryset = self._queryset.filter(**{f"{field}__lt": value})
        return self

    def _is_less_than_or_equal(self, field, value, **kwargs) -> Self:
        self._queryset = self._queryset.filter(**{f"{field}__lte": value})
        return self

    def _is_empty(self, field, value, **kwargs) -> Self:
        self._queryset = self._queryset.filter(Q(**{f"{field}__isnull": True}))
        return self

    def _is_not_empty(self, field, value, **kwargs) -> Self:
        self._queryset = self._queryset.exclude(**{f"{field}__isnull": True})
        return self


class FilterBooleanField(AbstractFilter):
    def _cast(self, value) -> Any:
        if not isinstance(value, (bool, int, str)):
            raise UnsupportedFilterType("Value must be a boolean")
        return Cast(value).to_bool()


class FilterCharField(AbstractFilter):
    OPERATORS_MAP = {
        "contains": "contains",
        "startsWith": "starts_with",
        "endsWith": "ends_with",
        "in": "in",
        "isAnyOf": "in",
        "not": "not",
        "isEmpty": "is_empty",
        "isNotEmpty": "is_not_empty",
        "doesNotEqual": "not",
        "doesNotContain": "does_not_contain",
    }

    def _cast(self, value) -> Any:
        if isinstance(value, (list, tuple)):
            return [str(v) for v in value]
        return str(value)

    def _eq(self, field, value, **kwargs) -> Self:
        self._queryset = self._queryset.filter(**{f"{field}__iexact": value})
        return self

    def _not(self, field, value, **kwargs) -> Self:
        self._queryset = self._queryset.exclude(**{f"{field}__iexact": value})
        return self

    def _in(self, field, value, **kwargs) -> Self:
        if not isinstance(value, (list, tuple)):
            value = (value,)
        value = [v.lower() for v in value]
        self._queryset = self._queryset.annotate(lower=Lower(field)).filter(**{"lower__in": value})
        return self

    def _contains(self, field, value, **kwargs) -> Self:
        self._queryset = self._queryset.filter(**{f"{field}__icontains": value})
        return self

    def _does_not_contain(self, field, value, **kwargs) -> Self:
        self._queryset = self._queryset.exclude(**{f"{field}__icontains": value})
        return self

    def _starts_with(self, field, value, **kwargs) -> Self:
        self._queryset = self._queryset.filter(**{f"{field}__istartswith": value})
        return self

    def _ends_with(self, field, value, **kwargs) -> Self:
        self._queryset = self._queryset.filter(**{f"{field}__iendswith": value})
        return self

    def _is_empty(self, field, value, **kwargs) -> Self:
        self._queryset = self._queryset.filter(Q(**{f"{field}__isnull": True}) | Q(**{f"{field}__exact": ""}))
        return self

    def _is_not_empty(self, field, value, **kwargs) -> Self:
        self._queryset = self._queryset.exclude(**{f"{field}__isnull": True}).exclude(**{f"{field}__exact": ""})
        return self


class FilterDateField(AbstractFilter):
    ANNOTATION_NAME = "df"
    FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"
    OPERATORS_MAP = {
        "not": "not",
        "in": "in",
        "after": "is_grater_than",
        "onOrAfter": "is_grater_than_or_equal",
        "before": "is_less_than",
        "onOrBefore": "is_less_than_or_equal",
        "isEmpty": "is_empty",
        "isNotEmpty": "is_not_empty",
    }

    def _cast(self, value, **kwargs) -> Any:
        if isinstance(value, (list, tuple)):
            return [Cast(v).to_date_time(kwargs.get("format")) for v in value]
        return Cast(value).to_date_time(kwargs.get("format"))

    def _cast_field_and_annotate(self, field) -> dict:
        return {self.ANNOTATION_NAME: DjangoCast(field, output_field=DateField())}

    def query_filter(self, field, value, operator="eq", **kwargs) -> Self:
        annotation = self._cast_field_and_annotate(field)
        value = (
            self._cast(value, format=kwargs.get("format", self.FORMAT))
            if operator not in ("isEmpty", "isNotEmpty")
            else value
        )

        operator = self._map_operator(operator)
        self._queryset = self._queryset.annotate(**annotation)
        return getattr(self, f"_{operator}")(self._resolve_field_name(field), value, **kwargs)


class FilterDateTimeField(FilterDateField):
    FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"

    def _cast_field_and_annotate(self, field) -> dict:
        return {"df": DjangoCast(field, output_field=DateTimeField())}


class FilterIntegerField(AbstractFilter):
    OPERATORS_MAP = {
        "!=": "not",
        "in": "in",
        ">": "is_grater_than",
        ">=": "_is_grater_than_or_equal",
        "<": "is_less_than",
        "<=": "is_less_than_or_equal",
        "isEmpty": "is_empty",
        "isNotEmpty": "is_not_empty",
        "isAnyOf": "in",
    }

    def _cast(self, value) -> Any:
        if isinstance(value, (list, tuple)):
            return [Cast(v).to_int() for v in value]
        return Cast(value).to_int()


class FilterTextField(FilterCharField):
    pass


class Filter(ABC):
    FILTERS_MAP = {
        "integerfield": FilterIntegerField,
        "datetimefield": FilterDateTimeField,
        "datefield": FilterDateField,
        "booleanfield": FilterBooleanField,
        "charfield": FilterCharField,
        "textfield": FilterTextField,
    }

    CUSTOM_FILTERS_MAP = {}

    FIELDS_MAP = {}

    CUSTOM_FIELDS = {}

    def __init__(self, queryset: QuerySet, field: str, value: Any, operator: str = ""):
        self._queryset = queryset
        self._value = value
        self._operator = operator

        try:
            self._field = self._map_fields(field)
            self._type = self._map_type(field)
            self._field_name = "".join(field.split("_")).lower()
        except FieldDoesNotExist:
            raise UnsupportedFilterType("Field does not exist")

    def _map_fields(self, field):
        field = self.FILTERS_MAP.get(field, field)
        field = field if self.CUSTOM_FIELDS.get(field) else self._queryset.model._meta.get_field(field).name
        return field

    def _map_type(self, field):
        return self.CUSTOM_FIELDS.get(field) or self._queryset.model._meta.get_field(field).get_internal_type()

    @abstractmethod
    def query_filter(self, **kwargs) -> QuerySet:
        pass

    @classmethod
    def filter_multiple(cls, queryset: QuerySet, filters: Iterable[dict]) -> QuerySet:
        queryset = queryset.all()
        for query_filter in filters:
            field = query_filter.pop("field")
            value = query_filter.pop("value")
            operator = query_filter.pop("operator", "")
            if isinstance(value, str):
                value = value.strip()
            queryset = cls(queryset, field, value, operator).query_filter(**query_filter)
        return queryset
