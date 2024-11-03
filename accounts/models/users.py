from django.db import models
from django.contrib.auth.models import AbstractBaseUser, PermissionsMixin
from ..managers import UserManager
from accounts.models.base import TimeStampedModel

import uuid


class User(TimeStampedModel, AbstractBaseUser, PermissionsMixin):
    email = models.EmailField(
        verbose_name="email address",
        max_length=255,
        unique=True,
    )
    phone_number = models.CharField(
        verbose_name="Phone Number",
        max_length=11,
        unique=True
    )
    username = models.CharField(
        max_length=255,
        blank=True,
        null=True,
    )

    is_active = models.BooleanField(default=True)
    is_admin = models.BooleanField(default=False)

    objects = UserManager()

    USERNAME_FIELD = "phone_number"
    REQUIRED_FIELDS = ["email"]

    class Meta:
        verbose_name = "User "
        verbose_name_plural = "Users"

    def __str__(self):
        return self.email

    @property
    def is_staff(self):
        return self.is_admin

    def save(self, *args, **kwargs):
        if not self.username:
            self.username = self.email.split("@")[0]
            if User.objects.filter(username=self.username).exists():
                self.username = f"{self.username}-{uuid.uuid4().hex[:8]}"
        super().save(*args, **kwargs)
