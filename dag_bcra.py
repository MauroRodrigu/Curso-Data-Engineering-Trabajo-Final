import pandas as pd
import requests 
import json
import sqlalchemy as sa
from configparser import ConfigParser
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def credenciales(config_ruta, config_seccion):
    parser = ConfigParser()
    parser.read(config_ruta)
    config = parser[config_seccion]
    return config

config = credenciales('config.ini', 'redshift')

