from django.db import models
from django.utils import timezone

from accounts.models.base import TimeStampedModel


class OtpCode(TimeStampedModel):
    """
    Represents a one-time password (OTP) code associated with a phone number.

    Attributes:
        phone_number (str): The phone number associated with the OTP code. This field is unique.
        code (int): The one-time password code, stored as a positive small integer.
        created_at (datetime): The timestamp indicating when the OTP code was created. Inherited from TimeStampedModel.

    Methods:
        __str__(): Returns a string representation of the OTP code, including the phone number, code, and creation time.
        is_valid(): Checks if the OTP code is still valid based on its creation time. The code is valid for 60 seconds after creation.
    """
    phone_number = models.CharField(max_length=11, unique=True)
    code = models.PositiveSmallIntegerField()

    def __str__(self):
        return f"{self.phone_number} - {self.code} - {self.created_at}"

    def is_valid(self):
        time_diff = timezone.now() - self.created_at
        return time_diff.total_seconds() < 60
