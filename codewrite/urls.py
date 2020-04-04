from django.urls import path
from . import views

urlpatterns = [
    path('', views.index, name='index'),
    path('check_finish', views.check_finish, name='check_finish'),
    path('concatination', views.concatination, name='concatination'),
]
