from django import forms
from django.contrib.auth.forms import ReadOnlyPasswordHashField
from django.core.exceptions import ValidationError
from .models import User, OtpCode


class UserCreationFrom(forms.ModelForm):
    password1 = forms.CharField(
        label="Password",
        widget=forms.PasswordInput
    )
    password2 = forms.CharField(
        label="Password confirmation",
        widget=forms.PasswordInput
    ) 
    
    class Meta:
        model = User
        fields = ["phone_number", "email", "full_name"]
        
        
    def clean_password2(self):
        cd = self.cleaned_data
        if cd['password1'] and cd['password2'] and cd['password1'] != cd['password2']:
            raise ValidationError("Password don't match")
        return cd['password2']
    
    
    def save(self, commit=True):
        user = super().save(commit=False)
        user.set_password(self.cleaned_data["password1"])
        if commit:
            user.save()

        return user
    

class UserChangeForm(forms.ModelForm):
    password = ReadOnlyPasswordHashField(help_text = "you can change password using <a href=\"../password\">this form</a>.")
    
    class Meta:
        model = User
        fields = ["phone_number", "email", "full_name", "password", "is_active", "is_admin", "last_login"]
        
        
        
class UserRegistrationForm(forms.Form):
    email = forms.EmailField()
    full_name = forms.CharField(label="Full Name", max_length=255)
    phone = forms.CharField(max_length=11)
    password = forms.CharField(widget=forms.PasswordInput)

    
    def clean_email(self):
        email = self.cleaned_data['email']
        user = User.objects.filter(email=email).exists()
        if user:
            raise ValidationError("this mail already exists")
        return email
    
    
    def clean_phone(self):
        phone = self.data.get('phone')
        if User.objects.filter(phone_number=phone).exists():
            raise ValidationError("this phone number already exists")
        OtpCode.objects.filter(phone_number=phone).delete()
        return phone


class VerifyCodeForm(forms.Form):
    code = forms.IntegerField(min_value=100, max_value=9999)
    
    
    
class UserLoginForm(forms.Form):
    phone = forms.CharField(max_length=11)
    password = forms.CharField(widget=forms.PasswordInput)
    