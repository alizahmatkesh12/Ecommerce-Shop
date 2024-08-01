from django.core.management.base import BaseCommand, CommandError
from accounts.models import OtpCode
from datetime import datetime, timedelta
import pytz
# from django.utils import timezone
"""
    IN THERE WE NEED TO USE TIMEZONE FROM DJANGO.UTILS 
        BECAUSE WE GET WARRING DURIN RUNNING THISS COMMAND AND
        EVERYTIME WE MAKEMIGRATIONS IT'S MAEK A MIGRATE FOR US
    
"""



class Command(BaseCommand):

    help = "Remove all expried OTP codes"
    
    def handle(self, *args, **options):
        expired_time = datetime.now(tz=pytz.timezone('Asia/Tehran')) - timedelta(minutes=2)
        OtpCode.objects.filter(created_at__lt = expired_time).delete()
        self.stdout.write("All expired otp codes removed")
        