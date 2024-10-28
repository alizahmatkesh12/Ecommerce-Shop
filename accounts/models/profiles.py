from django.db import models
from django.db.models.signals import post_save
from django.dispatch import receiver
from django.contrib.auth import get_user_model


from accounts.models.base import TimeStampedModel



class Profile(TimeStampedModel):
    """
    Profile class for each user which is being created to hold the information
    """
    user = models.ForeignKey(get_user_model(), on_delete=models.CASCADE, related_name="profile")
    first_name = models.CharField(max_length=250)
    last_name = models.CharField(max_length=250)
    image = models.ImageField(blank=True, null=True)
    about = models.TextField(blank=True, null=True)

    def __str__(self):
        return self.user.email



@receiver(post_save, sender=get_user_model())
def save_profile(sender, instance, created, **kwargs):
    """
    Signal for post creating a user which activates when a user being created ONLY
    """
    if created:
        Profile.objects.create(user=instance)