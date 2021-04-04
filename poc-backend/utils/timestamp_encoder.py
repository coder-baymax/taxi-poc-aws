import datetime
from math import ceil
import time

from django.core.serializers.json import DjangoJSONEncoder
from utils.custom_model import EnumValue


class TimestampEncoder(DjangoJSONEncoder):
    def default(self, o):
        if isinstance(o, datetime.datetime):
            return ceil(o.timestamp() * 1000)
        elif isinstance(o, datetime.date):
            return time.mktime(o.timetuple())
        elif isinstance(o, EnumValue):
            return o.identify
        else:
            return super(TimestampEncoder, self).default(o)
