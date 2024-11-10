from django.shortcuts import render, redirect, get_object_or_404
from django.views import View
from django.contrib import messages
from django.contrib.auth import login, logout, authenticate
from django.contrib.auth.mixins import LoginRequiredMixin
from django.contrib.auth.hashers import make_password

from accounts.forms import (
    UserRegistrationForm,
    VerifyCodeForm,
    UserLoginForm,
    EditProfileForm,
    AddressForm,
)

from accounts.models.users import User
from accounts.models.otp_code import OtpCode
from accounts.models.profiles import Profile
from accounts.models.address import Address
from orders.models import Order
from accounts.utils import send_otp_code

import random


class UserRegisterView(View):
    form_class = UserRegistrationForm
    template_name = "accounts/register.html"

    def get(self, request):
        form = self.form_class()
        return render(request, self.template_name, {"form": form})

    def post(self, request):
        form = self.form_class(request.POST)
        if form.is_valid():
            random_code = random.randint(1000, 9999)
            send_otp_code(form.cleaned_data["phone"], random_code)
            OtpCode.objects.create(
                phone_number=form.cleaned_data["phone"], code=random_code
            )
            request.session["user_registration_info"] = {
                "phone_number": form.cleaned_data["phone"],
                "email": form.cleaned_data["email"],
                "username": form.cleaned_data["username"],
                "password": make_password(form.cleaned_data["password"]),
            }
            request.session.set_expiry(300)

            messages.success(request, "We send you a code", "success")
            return redirect("accounts:verify_code")
        return render(request, self.template_name, {"form": form})


class UserRegisterVerifyCodeView(View):
    form_class = VerifyCodeForm

    def get(self, request):
        form = self.form_class()
        return render(request, "accounts/verify.html", {"form": form})

    def post(self, request):
        user_session = request.session.get("user_registration_info")
        if not user_session:
            messages.error(request, "Session expired. Please try registering again.")
            return redirect("accounts:user_register")
        try:
            code_instance = OtpCode.objects.get(
                phone_number=user_session["phone_number"]
            )
        except OtpCode.DoesNotExist:
            messages.error(request, "OTP code not found. Please request a new code.")
            return redirect("accounts:user_register")
        if not code_instance.is_valid():
            code_instance.delete()
            messages.error(request, "Your code has expired", "danger")
            return redirect("accounts:verify_code")
        form = self.form_class(request.POST)
        if form.is_valid():
            cd = form.cleaned_data
            if cd["code"] == code_instance.code:
                user = User.objects.create(
                    phone_number=user_session["phone_number"],
                    email=user_session["email"],
                    username=user_session["username"],
                    password=user_session["password"],
                )
                code_instance.delete()
                login(request, user)
                messages.success(request, "you registered", "success")
                return redirect("home:home")
            else:
                messages.error(request, "Your code is wrong", "danger")
                return redirect("accounts:verify_code")
        return redirect("home:home")


class UserLogoutView(LoginRequiredMixin, View):
    def get(self, request):
        logout(request)
        messages.success(request, "you logged out successfully", "success")
        return redirect("home:home")


class UserLoginView(View):
    form_class = UserLoginForm
    template_name = "accounts/login.html"

    def get(self, request):
        form = self.form_class()
        return render(request, self.template_name, {"form": form})

    def post(self, request):
        form = self.form_class(request.POST)
        if form.is_valid():
            cd = form.cleaned_data
            user = authenticate(
                request, phone_number=cd["phone"], password=cd["password"]
            )
            if user is not None:
                login(request, user)
                messages.success(request, "you logged in successfully", "info")
                return redirect("home:home")
            messages.error(request, "phone or password is wrong", "warning")
        return render(request, self.template_name, {"form": form})


class UserProfileView(LoginRequiredMixin, View):
    """
    Provides a form for users to update their profile information.
    """

    template_name = "accounts/profile.html"

    def get(self, request, user_id):
        profile = get_object_or_404(Profile, pk=user_id)
        order = Order.objects.filter(user=user_id).first()
        address = Address.objects.filter(user=profile.user)
        if request.user != profile.user:
            messages.error(request, "You do not have permission to view this profile.")
            return redirect("home:home")

        return render(
            request,
            self.template_name,
            {"profile": profile, "address": address, "order": order},
        )


class ProfileEditView(LoginRequiredMixin, View):
    """
    Provides a form for users to edit their profile information.
    """

    template_name = "accounts/edit_profile.html"
    form_class = EditProfileForm

    def get(self, request):
        profile = get_object_or_404(Profile, user=request.user.id)
        form = self.form_class(instance=profile)
        return render(request, self.template_name, {"form": form})

    def post(self, request):
        profile = get_object_or_404(Profile, user=request.user)
        form = self.form_class(request.POST, request.FILES, instance=profile)

        if form.is_valid():
            form.save()
            messages.success(request, "Your profile has been updated successfully.")
            return redirect("accounts:user_profile", user_id=request.user.id)

        messages.error(
            request,
            "There was an error updating your profile. Please correct the errors below.",
        )
        return render(request, self.template_name, {"form": form})


class AddAddressView(LoginRequiredMixin, View):

    def get(self, request):
        form = AddressForm()
        return render(request, "accounts/address.html", {"form": form})

    def post(self, request):
        form = AddressForm(request.POST)

        if form.is_valid():
            address = form.save(commit=False)
            address.user = request.user
            address.save()
            return redirect("accounts:user_profile", user_id=request.user.id)

        return render(request, "accounts/address.html", {"form": form})


class EditAddressView(LoginRequiredMixin, View):
    """
    Provides a form to edit an existing address for the user.
    """

    def get(self, request, id):
        address = get_object_or_404(Address, id=id, user=request.user)
        form = AddressForm(instance=address)
        return render(
            request, "accounts/address.html", {"form": form, "address": address}
        )

    def post(self, request, id):
        address = get_object_or_404(Address, id=id, user=request.user)
        form = AddressForm(request.POST, instance=address)
        if form.is_valid():
            form.save()
            messages.success(request, "آدرس با موفقیت ویرایش شد.")
            return redirect("accounts:user_profile", user_id=request.user.id)
        return render(
            request, "accounts/address.html", {"form": form, "address": address}
        )


class DeleteAddressView(LoginRequiredMixin, View):
    """
    Provides the functionality to delete an address.
    """

    def get(self, request, id):
        address = get_object_or_404(Address, id=id, user=request.user)
        address.delete()
        messages.success(request, "آدرس با موفقیت حذف شد.")
        return redirect("accounts:user_profile", user_id=request.user.id)
