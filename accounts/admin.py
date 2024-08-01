from django.contrib import admin
from django.contrib.auth.models import Group
from django.contrib.auth.admin import UserAdmin as BaseUserAdmin
from .forms import UserChangeForm, UserCreationFrom    
from .models import User, OtpCode


@admin.register(OtpCode)
class OtpCode(admin.ModelAdmin):
    list_display = ["phone_number", "code", "created_at"]


class UserAdmin(BaseUserAdmin):
    form = UserChangeForm
    add_form = UserCreationFrom
    
    list_display = ["phone_number", "email", "full_name", "is_admin"]
    list_filter = ["is_admin"]
    fieldsets = [
        (None, {"fields": ["phone_number", "email", "full_name", "password"]}),
        ("Permissions", {"fields": ["is_admin", "is_active", "last_login"]}),
    ]
    add_fieldsets = [
        (None, {"fields": ["phone_number", "email", "full_name", "password1", "password2"]}),
    ]
    search_fields = ["email"]
    ordering = ["email"]
    filter_horizontal = []
    
admin.site.register(User, UserAdmin)
admin.site.unregister(Group)