import itertools
from itertools import chain

from django.db import models
from django.db.models.fields import DateField
from utils.custom_model.enum_field import EnumValue


class JsonModelMeta(models.base.ModelBase):
    def __new__(mcs, what, bases=None, attrs=None):
        new_class = super().__new__(mcs, what, bases, attrs)
        if what != "JsonModel":
            field_names, field_map = set(), {}
            meta = new_class._meta
            for field in itertools.chain(
                meta.local_fields, meta.local_many_to_many, meta.private_fields
            ):
                field_names.add(field.name)
                if field.is_relation and not isinstance(
                    field.remote_field, models.ManyToManyRel
                ):
                    field_names.add("{}_id".format(field.name))
                    field_map[field.name] = "{}_id".format(field.name)
            setattr(new_class, "field_names", field_names)
            setattr(new_class, "field_map", field_map)
            setattr(
                new_class,
                "filter_kwargs",
                classmethod(
                    lambda cls, kwargs: {
                        k: v for k, v in kwargs.items() if k in new_class.field_names
                    }
                ),
            )
        return new_class


class JsonModel(models.Model, metaclass=JsonModelMeta):
    DUMP_DROP = ["id"]

    def trans_json(self, json_obj):
        def trans_single(v):
            return v.json if isinstance(v, EnumValue) or isinstance(v, JsonModel) else v

        json_dict = {}
        for key, value in json_obj.items():
            key = self.field_map.get(key, key)
            if isinstance(value, list):
                json_dict[key] = [trans_single(x) for x in value]
            elif isinstance(value, dict):
                json_dict[key] = self.trans_json(value)
            else:
                json_dict[key] = trans_single(value)
        return json_dict

    @property
    def json(self):
        return self.trans_json(self.model_to_dict(self))

    @property
    def dump(self):
        json_obj = self.json
        for name in self.DUMP_DROP:
            json_obj.pop(name, None)
        return json_obj

    @staticmethod
    def model_to_dict(instance, fields=None, exclude=None):
        """ "
        django.forms.models.model_to_dict补丁版本
        后续如果有需要再把这个方法抽取出去
        """
        opts = instance._meta
        data = {}
        for f in chain(opts.concrete_fields, opts.private_fields, opts.many_to_many):
            # TODO 如果是Date字段，也可以进行处理
            if not getattr(f, "editable", False) and not isinstance(f, DateField):
                continue
            if fields is not None and f.name not in fields:
                continue
            if exclude and f.name in exclude:
                continue
            data[f.name] = f.value_from_object(instance)
        return data

    class Meta:
        abstract = True
