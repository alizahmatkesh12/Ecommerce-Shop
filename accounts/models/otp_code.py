from django.db import models
from .base import BaseModel
from django.utils import timezone


class OtpCode(BaseModel):
    phone_number = models.CharField(max_length=11, unique=True)
    code = models.PositiveSmallIntegerField()
    
    def __str__(self) -> str:
        return f"{self.phone_number} - {self.code} - {self.created_at}"
    
    def is_valid(self):
        time_diff = timezone.now() - self.created_at
        return time_diff.total_seconds() < 60
