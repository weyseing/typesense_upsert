from django.urls import path
from . import views

urlpatterns=[
    path('transaction',views.transaction),
    path('health',views.healthcheck)
]