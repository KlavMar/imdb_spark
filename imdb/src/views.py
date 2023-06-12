from django.shortcuts import render
from dotenv import load_dotenv
from dotenv import dotenv_values
import json
from src.connection_db import ConnectionDb
import os
import pandas as pd 
import numpy as np
from pathlib import Path
from src.app_dash import imdb_dash

# Create your views here.


def get_view_imdb(request):
        return render(request,"app_imdb.html")

