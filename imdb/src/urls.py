from django.shortcuts import render
from django.urls import path
from src import views

urlpatterns=[

    path("",views.get_view_imdb,name="app_imdb"),
]
