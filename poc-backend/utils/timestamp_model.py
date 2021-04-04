from django.db import models
from utils.custom_model import JsonModel


class TimestampModel(JsonModel):
    created_time = models.DateTimeField(auto_now_add=True)
    updated_time = models.DateTimeField(auto_now=True)

    class Meta:
        abstract = True
