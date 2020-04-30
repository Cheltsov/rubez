from django.urls import path
from . import views

urlpatterns = [
    path('', views.index, name='index'),
    path('get_skpdi', views.get_skpdi, name='get_skpdi'),
    path('get_stat_gipdd', views.get_stat_gipdd, name='get_stat_gipdd'),
    path('get_collision', views.get_collision, name='get_collision'),
    path('get_dtp_id', views.get_dtp_id, name='get_dtp_id'),
    path('get_hearth', views.get_hearth, name='get_hearth'),
    path('get_hearth_tmp', views.get_hearth_tmp, name='get_hearth_tmp')
]
