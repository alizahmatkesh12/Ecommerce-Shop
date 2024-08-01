from django.db import models
from django.contrib.auth.models import BaseUserManager


class UserManager(BaseUserManager):
    def create_user(self, phone_number, email, full_name, password=None):
        if not phone_number:
            raise ValueError("User must have a phone number")
        
        if not email:
            raise ValueError("Users must have an email address")
        
        if not full_name:
            raise ValueError("Users must have a name")
        
        user = self.model(
            phone_number = phone_number,
            email = self.normalize_email(email), 
            full_name = full_name,
        )
        user.set_password(password)
        user.save(using=self._db)
        return user
    
    def create_superuser(self, phone_number, email, full_name, password=None):
        
        user = self.create_user(
            phone_number,
            email,
            full_name,
            password,
        )
        user.is_admin = True
        user.save(using=self._db)
        return user
    
    