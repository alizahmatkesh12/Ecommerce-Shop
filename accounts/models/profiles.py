from django.db import models
from django.db.models.signals import post_save
from django.dispatch import receiver
from django.contrib.auth import get_user_model


from accounts.models.base import TimeStampedModel


class Profile(TimeStampedModel):
    """
    Profile class for each user which is being created to hold the information
    """

    user = models.OneToOneField(
        get_user_model(), on_delete=models.CASCADE, related_name="profile"
    )
    first_name = models.CharField(max_length=250)
    last_name = models.CharField(max_length=250)
    image = models.ImageField(upload_to="accounts/", default="default-avatar.png")
    about = models.TextField(blank=True, null=True)

    def __str__(self):
        return self.user.email


@receiver(post_save, sender=get_user_model())
def manage_profile(sender, instance, created, **kwargs):
    """
    Signal to create or save the Profile object whenever the User object is created or saved.
    """
    if created:
        Profile.objects.create(user=instance)
    else:
        instance.profile.save()
