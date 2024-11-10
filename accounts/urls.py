from django.urls import path
from . import views


app_name = 'accounts'
urlpatterns = [
    path('register/', views.UserRegisterView.as_view(), name='user_register'),
    path('verify/', views.UserRegisterVerifyCodeView.as_view(), name='verify_code'),
    path('login/', views.UserLoginView.as_view(), name='user_login'),
    path('logout/', views.UserLogoutView.as_view(), name='user_logout'),
    path('profile/<int:user_id>/', views.UserProfileView.as_view(), name='user_profile'),
    path('profile/edit/', views.ProfileEditView.as_view(), name='profile_edit'),
    path('add-address/', views.AddAddressView.as_view(), name="add-address"),
    path('address/edit/<int:id>/', views.EditAddressView.as_view(), name='edit_address'),
    path('address/delete/<int:id>/', views.DeleteAddressView.as_view(), name='delete_address'),
]