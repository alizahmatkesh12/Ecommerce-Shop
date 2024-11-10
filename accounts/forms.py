from django import forms
from django.contrib.auth.forms import ReadOnlyPasswordHashField
from django.core.exceptions import ValidationError

from accounts.models.users import User
from accounts.models.otp_code import OtpCode
from accounts.models.profiles import Profile
from accounts.models.address import Address


class UserCreationFrom(forms.ModelForm):
    password1 = forms.CharField(label="Password", widget=forms.PasswordInput)
    password2 = forms.CharField(
        label="Password confirmation", widget=forms.PasswordInput
    )

    class Meta:
        model = User
        fields = ["phone_number", "email", "username"]

    def clean_password2(self):
        cd = self.cleaned_data
        if cd["password1"] and cd["password2"] and cd["password1"] != cd["password2"]:
            raise ValidationError("Password don't match")
        return cd["password2"]

    def save(self, commit=True):
        user = super().save(commit=False)
        user.set_password(self.cleaned_data["password1"])
        if commit:
            user.save()

        return user


class UserChangeForm(forms.ModelForm):
    password = ReadOnlyPasswordHashField(
        help_text='you can change password using <a href="../password">this form</a>.'
    )

    class Meta:
        model = User
        fields = [
            "phone_number",
            "email",
            "username",
            "password",
            "is_active",
            "is_admin",
            "last_login",
        ]


class UserRegistrationForm(forms.Form):
    email = forms.EmailField()
    username = forms.CharField(label="Full Name", max_length=255)
    phone = forms.CharField(max_length=11)
    password = forms.CharField(widget=forms.PasswordInput)

    def clean_email(self):
        email = self.cleaned_data["email"]
        user = User.objects.filter(email=email).exists()
        if user:
            raise ValidationError("this mail already exists")
        return email

    def clean_phone(self):
        phone = self.data.get("phone")
        if User.objects.filter(phone_number=phone).exists():
            raise ValidationError("this phone number already exists")
        OtpCode.objects.filter(phone_number=phone).delete()
        return phone


class VerifyCodeForm(forms.Form):
    code = forms.IntegerField(min_value=100, max_value=9999)


class UserLoginForm(forms.Form):
    phone = forms.CharField(max_length=11)
    password = forms.CharField(widget=forms.PasswordInput)


class EditProfileForm(forms.ModelForm):
    class Meta:
        model = Profile
        fields = ["first_name", "last_name", "image", "about"]


class AddressForm(forms.ModelForm):
    class Meta:
        model = Address
        fields = [
            "city",
            "street_address",
            "postal_code",
            "phone_number",
            "building_number",
            "apartment_number",
        ]

        def clean_address_id(self):
            address_id = self.cleaned_data.get("address_id")

            # Check if the address_id belongs to the requested user
            user = (
                self.request.user
            )  # Assuming the user is available in the request object
            try:
                address = Address.objects.get(id=address_id, user=user)
            except Address.DoesNotExist:
                raise forms.ValidationError("Invalid address for the requested user.")

    widgets = {
        "city": forms.TextInput(attrs={"placeholder": "Enter your city"}),
        "street_address": forms.TextInput(
            attrs={"placeholder": "Enter your street address"}
        ),
        "postal_code": forms.TextInput(attrs={"placeholder": "Enter your postal code"}),
        "phone_number": forms.TextInput(
            attrs={"placeholder": "Enter your phone number"}
        ),
        "building_number": forms.NumberInput(attrs={"placeholder": "Building number"}),
        "apartment_number": forms.NumberInput(
            attrs={"placeholder": "Apartment number"}
        ),
    }

    def clean_phone_number(self):
        phone_number = self.cleaned_data.get("phone_number")
        if not phone_number.isdigit():
            raise forms.ValidationError("Phone number must contain only digits.")
        if len(phone_number) < 10:
            raise forms.ValidationError("Phone number must be at least 10 digits long.")
        return phone_number
