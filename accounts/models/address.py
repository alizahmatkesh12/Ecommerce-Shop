from django.db import models
from django.contrib.auth import get_user_model
from django.core.validators import MinValueValidator

from accounts.models.base import TimeStampedModel


class Address(TimeStampedModel):
    """
    Represents a physical address associated with a user.
    """
    user = models.ForeignKey(get_user_model(), related_name="address", on_delete=models.CASCADE)
    city = models.CharField(max_length=100, blank=False, null=False)
    street_address = models.CharField(max_length=250, blank=False, null=False)
    postal_code = models.CharField(max_length=20, blank=True, null=True)
    phone_number = models.CharField(max_length=15, blank=True, null=True)
    building_number = models.IntegerField(
        blank=True, null=True, validators=[MinValueValidator(1)]
    )
    apartment_number = models.IntegerField(
        blank=True, null=True, validators=[MinValueValidator(1)]
    )

    class Meta:
        verbose_name = "Address"
        verbose_name_plural = "Addresses"

    def __str__(self):
        return f"{self.user} - {self.street_address}, {self.city}"
