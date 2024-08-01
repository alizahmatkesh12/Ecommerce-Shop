from django.db import models
from django.contrib.auth.models import  AbstractBaseUser
from .managers import UserManager
from django.utils import timezone


class User(AbstractBaseUser):
    email = models.EmailField(
        verbose_name="email address",
        max_length=255,
        unique=True,
    )
    phone_number = models.CharField(
        max_length=11,
        unique=True
    )
    full_name = models.CharField(
        max_length=255
    )
    
    is_active = models.BooleanField(default=True)
    is_admin = models.BooleanField(default=False)
    
    objects = UserManager()
    
    USERNAME_FIELD = "phone_number"
    REQUIRED_FIELDS = ["email", "full_name"]
    
    def __str__(self) -> str:
        return self.email
    
    def has_perm(self, perm, obj=None):
        return True
    
    def has_module_perms(self, app_label):
        return True
    
    @property
    def is_staff(self):
        return self.is_admin
    
    
class OtpCode(models.Model):
    phone_number = models.CharField(max_length=11, unique=True)
    code = models.PositiveSmallIntegerField()
    created_at = models.DateTimeField(default=timezone.now)
    
    
    def __str__(self) -> str:
        return f"{self.phone_number} - {self.code} - {self.created_at}"
    
    
    def is_valid(self):
        time_diff = timezone.now() - self.created_at
        return time_diff.total_seconds() < 60 