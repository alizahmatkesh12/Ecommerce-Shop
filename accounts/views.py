from django.shortcuts import render, redirect
from django.views import View
from .forms import UserRegistrationForm, VerifyCodeForm, UserLoginForm
import random
from utils import send_otp_code
from .models import OtpCode, User
from django.contrib import messages
from django.contrib.auth import login, logout, authenticate
from django.contrib.auth.mixins import LoginRequiredMixin

class UserRegisterView(View):
    form_class = UserRegistrationForm
    template_name = 'accounts/register.html'
    
    def get(self, request):
        form = self.form_class
        return render(request, self.template_name, {'form':form})
    
    def post(self, request):
        form = self.form_class(request.POST)
        if form.is_valid():
            random_code = random.randint(1000, 9999)
            send_otp_code(form.cleaned_data['phone'], random_code)
            OtpCode.objects.create(phone_number=form.cleaned_data['phone'], code=random_code)
            request.session['user_registration_info'] = {
                'phone_number': form.cleaned_data['phone'],
                'email': form.cleaned_data['email'],
                'full_name': form.cleaned_data['full_name'],
                'password': form.cleaned_data['password']
            }
            messages.success(request, "We send you a code", 'success')
            return redirect('accounts:verify_code')
        return render(request, self.template_name, {'form': form})
    
    
    
class UserRegisterVerifyCodeView(View):
    form_class = VerifyCodeForm
    
    def get(self, request):
        form = self.form_class
        return render(request, 'accounts/verify.html', {"form": form})
        
    
    def post(self, request):
        user_session =request.session['user_registration_info']
        code_instance = OtpCode.objects.get(phone_number = user_session['phone_number'])
        if not code_instance.is_valid():
            code_instance.delete()
            messages.error(request, "Your code has expired", 'danger')
            return redirect('accounts:verify_code')
        form = self.form_class(request.POST)
        if form.is_valid():
            cd = form.cleaned_data
            if cd['code'] == code_instance.code:
                User.objects.create(phone_number = user_session['phone_number'],
                                    email = user_session['email'],
                                    full_name = user_session['full_name'],
                                    password = user_session['password'],
                )
                code_instance.delete()
                messages.success(request, "you registered", 'success')
                return redirect('home:home')
            else:
                messages.error(request, "Your code is wrong", 'danger')
                return redirect('accounts:verify_code')
        return redirect('home:home')
    


class UserLogoutView(LoginRequiredMixin, View):
    def get(self, request):
        logout(request)
        messages.success(request, 'you logged out successfully', 'success')
        return redirect('home:home')


class UserLoginView(View):
    form_class = UserLoginForm
    template_name = 'accounts/login.html'
    
    def get(self, request):
        form = self.form_class
        return render(request, self.template_name, {'form': form})
    
    def post(self, request):
            form = self.form_class(request.POST)  
            if form.is_valid():
                cd = form.cleaned_data
                user = authenticate(request, phone_number=cd['phone'], password=cd['password'])
                if user is not None:
                    login(request, user)
                    messages.success(request, 'you logged in successfully', 'info')
                    return redirect('home:home')
                messages.error(request, 'phone or password is wrong', 'warning')
            return render(request, self.template_name, {'form':form})   



# class UserLoginVerifyCodeView(View):
#     form_class = VerifyCodeForm
#     template_name = 'accounts/login.html'


#     def get(self, request):
#         form = self.form_class
#         return render(request, 'accounts/verify.html', {'form': form})
    
#     def post(self, request):
#         form = self.form_class(request.Post)
#         if form.is_valid():
#             cd = form .cleaned_data
#             user = authenticate(request, phone_number = cd['phone'], password= cd['password'])
#             if user is not None:
#                 login(request, user)
#                 messages.success(request, 'Login was successfully', 'success')
#                 return redirect('home:home')
#             messages.error(request, 'PhoneNumber or Password was wrong', 'danger')
#         return render(request, self.template_name, {'form':form})











# class UserLoginView(View):
#     form_class = UserLoginForm
#     template_name = 'accounts/login.html'
    
#     def get(self, request):
#         form = self.form_class
#         return render(request, self.template_name, {'form': form})
    
    
#     def post(self, request):
#         form = self.form_class(request.POST)
#         if form.is_valid():
#             random_code = random.randint(1000, 9999)
#             send_otp_code(form.cleaned_data['phone'], random_code)
#             OtpCode.objects.create(phone_number=form.cleaned_data['phone'], code=random_code)
#             try:
#                 user = User.objects.get(phone_number = form.cleaned_data['phone'])
#                 if user.check_password(form.cleaned_data['password']):
#                     request.session['user_login_info'] = {
#                         'phone_number': form.cleaned_data['phone'],
#                         'email': user.email,
#                         'full_name': user.full_name,
#                         'password': form.cleaned_data['password']
#                     }
#                     messages.success(request, "We send you a code", 'success')
#                     return redirect('accounts:verify_code')
#                 else:
#                     messages.error(request, "Your password is wrong", 'dange')
#                     return redirect('accounts:verify_code')
#             except User.DoesNotExist:
#                 messages.error(request, "User does not exist", 'danger')
#                 return redirect('accounts:login')
#         return render(request, self.template_name, {'form': form})
        
        # user_session =request.session['user_login_info']
        # code_instance = OtpCode.objects.get(phone_number=user_session['phone_number'])
        # if not code_instance.is_valid():
        #     code_instance.delete()
        #     messages.error(request, "Your code has expired", 'danger')
        #     return redirect('accounts:login')
        # form = self.form_class(request.POST)
        # if form.is_valid():
        #     cd = form.cleaned_data
        #     if cd['code'] == code_instance.code:
        #         user = User.objects.get(phone_number=user_session['phone_number'])
        #         if user.check_password(user_session['password']):
        #         # user = authenticate(request, phone_number=user_session['phone_number'], password = user_session['password'])
        #         # if user is not None:    
        #             login(request, user)
        #             code_instance.delete()
        #             messages.success(request, "You logged in successfully", 'success')
        #             return redirect('home:home')
        #     else:
        #         messages.error(request, "Your code is wrong", 'danger')
        #         return redirect('accounts:login')
        # return redirect('home:home')           
    
    # # def post(self, request):
    # #     user_session =request.session['user_login_info']
    # #     code_instance = OtpCode.objects.get(phone_number=user_session['phone_number'])
    # #     if not code_instance.is_valid():
    # #         code_instance.delete()
    # #         messages.error(request, "Your code has expired", 'danger')
    # #         return redirect('accounts:login')
    # #     form = self.form_class(request.POST)
    # #     if form.is_valid():
    # #         cd = form.cleaned_data
    # #         if cd['code'] == code_instance.code:
    # #             # user = User.objects.get(phone_number=user_session['phone_number'])
    # #             # if user.check_password(user_session['password']):
    # #             user = authenticate(request, phone_number=user_session['phone_number'], password = user_session['password'])
    # #             if user is not None:    
    # #                 login(request, user)
    # #                 code_instance.delete()
    # #                 messages.success(request, "You logged in successfully", 'success')
    # #                 return redirect('home:home')
    # #             else:
    # #                 messages.error(request, "Your password is wrong", 'danger')
    # #                 return redirect('accounts:login')
    # #         else:
    # #             messages.error(request, "Your code is wrong", 'danger')
    # #             return redirect('accounts:login')
    # #     return redirect('home:home')
    
    
    # def post(self, request):
    #         user_session = request.session.get('user_login_info')
    #         if not user_session:
    #             messages.error(request, "Session expired. Please login again.", 'danger')
    #             return redirect('accounts:login')

    #         form = self.form_class(request.POST)
    #         if form.is_valid():
    #             try:
    #                 code_instance = OtpCode.objects.get(phone_number=user_session['phone_number'])
    #                 if not code_instance.is_valid():
    #                     code_instance.delete()
    #                     messages.error(request, "Your code has expired or is invalid", 'danger')
    #                     return redirect('accounts:login')
    #             except OtpCode.DoesNotExist:
    #                 messages.error(request, "OTP code does not exist", 'danger')
    #                 return redirect('accounts:login')

    #             if form.cleaned_data['code'] == code_instance.code:
    #                 user = authenticate(request, phone_number=user_session['phone_number'], password=user_session['password'])
    #                 if user:
    #                     login(request, user)
    #                     code_instance.delete()
    #                     messages.success(request, "You logged in successfully", 'success')
    #                     return redirect('home:home')
    #                 else:
    #                     messages.error(request, "Authentication failed. Please try again.", 'danger')
    #             else:
    #                 messages.error(request, "Your code is wrong", 'danger')
    #         return render(request, 'accounts/verify.html', {'form': form})