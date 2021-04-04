from collections import OrderedDict
import json
from typing import Any

from django.db import models

from utils.message_exception import MessageException


class EnumValue:
    def __init__(self, *args, identify_field=None, **kwargs):
        self.args = args
        self.identify_field = identify_field
        self.values = kwargs

    @property
    def str(self):
        return (
            list(self.values.values())[0]
            if len(self.values) == 1
            else json.dumps(self.values, sort_keys=True)
        )

    @property
    def identify(self):
        return self.values[self.identify_field]

    @property
    def json(self):
        if not self.values:
            return self.args
        elif len(self.values) == 1:
            return list(self.values.values())[0]
        else:
            result = {}
            for key, value in self.values.items():
                if isinstance(value, EnumValue):
                    value = value.json
                elif isinstance(value, list):
                    value = [x.json if isinstance(x, EnumValue) else x for x in value]
                result[key] = value
            return result

    def check(self, **query):
        for key, value in query.items():
            if value != self.values.get(key):
                return False
        return True

    def __getattribute__(self, name: str) -> Any:
        if name != "values" and hasattr(self, "values") and name in self.values:
            return self.values[name]
        return super().__getattribute__(name)


class EnumMeta(type):
    def __new__(mcs, what, bases=None, attrs=None):
        if what != "EnumField":
            field_list, value_dict = attrs["field_list"], OrderedDict()
            identify_field = field_list[0]
            for key, value in attrs.items():
                if isinstance(value, EnumValue):
                    attrs[key] = EnumValue(
                        identify_field=identify_field,
                        **{k: v for k, v in zip(field_list, value.args)}
                    )
                    value_dict[attrs[key].identify] = attrs[key]
            attrs["value_list"] = list(value_dict.values())
            attrs["value_dict"] = value_dict
            attrs["identify_field"] = identify_field
        return super().__new__(mcs, what, bases, attrs)


class EnumField(models.CharField, metaclass=EnumMeta):
    def __init__(self, *args, **kwargs):
        if "max_length" not in kwargs:
            kwargs["max_length"] = 255
        super().__init__(*args, **kwargs)

    @classmethod
    def get_all(cls, **query):
        return (
            [x for x in cls.value_dict.values() if x.check(**query)]
            if query
            else cls.value_list
        )

    @classmethod
    def from_value(cls, value):
        if isinstance(value, EnumValue) or value is None:
            return value
        elif value in cls.value_dict:
            return cls.value_dict[value]
        else:
            raise MessageException("wrong enum value")

    def from_db_value(self, value, expression, connection):
        return value if value is None else self.from_value(value)

    def to_python(self, value):
        return self.from_value(value)

    def get_prep_value(self, value):
        if isinstance(value, str):
            return value
        elif value is None:
            return None
        else:
            return value.identify
