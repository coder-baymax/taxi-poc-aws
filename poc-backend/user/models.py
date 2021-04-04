from django.contrib.auth.hashers import check_password, make_password
from django.db import models

from utils.timestamp_model import TimestampModel


class UserManager(models.Manager):
    def auth(self, username, clear_password):
        user = self.filter(username=username).first()
        if user and check_password(clear_password, user.password):
            return user
        else:
            return None

    def bulk_create(self, users, *args, **kwargs):
        for user in users:
            if not user.password.startswith("pbkdf2_sha256"):
                user.password = make_password(user.password)
        return super(UserManager, self).bulk_create(users, *args, **kwargs)


class UserRole:
    OPERATOR = "operator"
    DRIVER = "driver"


class User(TimestampModel):
    nick_name = models.CharField(blank=False, null=False, max_length=256)
    username = models.CharField(blank=False, null=True, unique=True, max_length=128)
    password = models.CharField(blank=False, null=False, max_length=256)
    session_id = models.CharField(blank=True, null=True, max_length=256)
    role = models.CharField(blank=False, null=False, default=UserRole.DRIVER, max_length=16)

    objects = UserManager()

    @property
    def json(self):
        return {k: v for k, v in super().json.items() if k not in {"password", "session_id"}}

    def save(self, *args, **kwargs):
        if not self.password.startswith("pbkdf2_sha256"):
            self.password = make_password(self.password)
        return super(User, self).save(*args, **kwargs)
