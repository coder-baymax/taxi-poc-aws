import json

from django.db import models
from utils.timestamp_encoder import TimestampEncoder


class JsonField(models.TextField):
    def from_db_value(self, value, expression, connection):
        return value if value is None else json.loads(value)

    def to_python(self, value):
        return json.loads(value) if isinstance(value, str) else value

    def get_prep_value(self, value):
        return json.dumps(value, sort_keys=True, cls=TimestampEncoder)
