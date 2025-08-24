from django.urls import path
from . import views

urlpatterns=[
    path('status_count_mins',views.status_count_mins),
    path('transaction',views.transaction),
    path('health',views.healthcheck)
]