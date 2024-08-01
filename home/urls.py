from django.urls import path, include
from . import views
from Eshop import settings
from django.conf.urls.static import static


app_name = 'home'


bucket_url = [
    path('', views.BucketHome.as_view(), name='bucket'),
    path('delete_obj/<str:key>/', views.DeleteBucketObject.as_view(), name='delete_obj_bucket'),
    path('download_obj/<str:key>/', views.DownloadBucketObject.as_view(), name='download_obj_bucket'),
]
urlpatterns = [
    path('', views.HomeView.as_view(), name='home'),
    path('Category/<str:category_slug>/', views.HomeView.as_view(), name='category_filter'),
    path('bucket/', include(bucket_url)),
    path('<slug:slug>/', views.PostDetailView.as_view(), name='product_detail'),
    
] + static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)