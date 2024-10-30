from django.db import models
from django.utils import timezone

from accounts.models.base import TimeStampedModel


class OtpCode(TimeStampedModel):
    """
    Represents a one-time password (OTP) code associated with a phone number.
    """
    VALIDITY_PERIOD = 60  # seconds
    
    phone_number = models.CharField(max_length=11, unique=True)
    code = models.PositiveSmallIntegerField()
    
    class Meta:
        verbose_name = "OTP Code"
        verbose_name_plural = "OTP Codes"

    def __str__(self):
        return f"{self.phone_number} - {self.code} - {self.created_at}"

    def is_valid(self):
        """
        Checks if the OTP code is still valid based on its creation time.
        """
        time_diff = timezone.now() - self.created_at
        return time_diff.total_seconds() < self.VALIDITY_PERIOD
