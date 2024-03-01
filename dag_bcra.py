import pandas as pd
import requests 
import json
import time
from datetime import datetime, timedelta, date
import sqlalchemy as sa
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from email.mime.text import MIMEText
import smtplib

def conexion_api(**context):
    
    códigos = ['usd', 'var_usd_vs_usd_of','base', 'reservas', 'base_div_res','circulacion_monetaria', 'depositos', 
       'cuentas_corrientes', 'tasa_adelantos_cuenta_corriente', 'cajas_ahorro', 'plazo_fijo', 'tasa_depositos_30_dias',
        'prestamos', 'tasa_prestamos_personales', 'porc_prestamos_vs_depositos','merval', 'merval_usd']
    
    nombres_variables = ['dolar_paralelo', 'brecha_dolar_oficial_paralelo', 'base_monetaria', 'reservas_internacionales',
                    'base_monetaria_dividida_reservas', 'circulacion_monetaria', 'depositos',
                    'cuentas_corrientes', 'tasa_adelantos_cuenta_corriente', 'cajas_ahorro', 'plazos_fijos',
                    'tasa_depositos_30_dias', 'prestamos', 'tasa_prestamos_personales', 'porc_prestamos_vs_depositos',
                         'merval', 'merval_usd']
    
    token = context['var']['value'].get("api_token")
    headers = dict(Authorization = token)
    
    api = 'https://api.estadisticasbcra.com/usd_of'
    data = requests.get(api, headers = headers).json()
    data = pd.DataFrame(data)
    data.columns = ['fecha', 'dolar_oficial']
    data['fecha'] = pd.to_datetime(data['fecha'], format = '%Y-%m-%d')
    
    for i in range(len(códigos)):
        api = 'https://api.estadisticasbcra.com/{}'.format(códigos[i])
        data2 = requests.get(api, headers = headers).json()
        data2 = pd.DataFrame(data2)
        data2.columns = ['fecha', nombres_variables[i]]
        data2['fecha'] = pd.to_datetime(data2['fecha'], format = '%Y-%m-%d')
        data = pd.merge(data, data2, how= 'outer')
        data = data.sort_values('fecha')
        time.sleep(1)
        
    data.drop_duplicates(inplace= True)
    data.dropna(inplace=True)
    
    data.to_csv('data/datos_bcra.csv', index= False)
    
    with open('data/mensaje.txt', 'w') as f:
        f.write("1. Conexión exitosa a la API.")

def filtrar_registros():

    engine = PostgresHook(postgres_conn_id = 'conn_redshift').get_sqlalchemy_engine()
    
    conn = engine.connect()
    
    ultima_fecha = pd.read_sql('select max(fecha) from rodriguez_mauro11_coderhouse.bcra', conn)
    
    data = pd.read_csv('data/datos_bcra.csv')
    
    print('ultima carga: {}'.format(ultima_fecha['max'].iloc[0]))
    
    if ultima_fecha['max'].iloc[0] == None:
        
        mensaje = """

2. No hay datos en redshift. Se insertarán los {} registros existentes de la API""".format(len(data))
        
        with open('data/mensaje.txt', 'a') as f:
            f.write(mensaje)
        
        conn.close()

    else:
        
        ultima_fecha = str(ultima_fecha['max'][0])
        data = data[data['fecha'] > ultima_fecha]

        if len(data) != 0:
            
            data.to_csv('data/datos_bcra.csv', index = False)
            
            mensaje = """

2. Existen {} registro/s nuevo/s que será/n cargardo/s en redshift""".format(len(data))
        
            with open('data/mensaje.txt', 'a') as f:
                f.write(mensaje)
            
            conn.close()
        else:
            mensaje = """

2. No hay registros nuevos para cargar en redshift."""
        
            with open('data/mensaje.txt', 'a') as f:
                f.write(mensaje)
            data = pd.DataFrame()
            data.to_csv('data/datos_bcra.csv', index = False)
            conn.close()
    
    
def conn_redshift():
    
    try:

        data = pd.read_csv('data/datos_bcra.csv')
    
        engine = PostgresHook(postgres_conn_id = 'conn_redshift').get_sqlalchemy_engine()
    
        conn = engine.connect()
    
        data.to_sql(name= 'bcra', con = conn, if_exists= 'append', method= 'multi', 
           chunksize= 1000, index= False)
        
        mensaje = """

3. Carga exitosa en Redshift."""
        
        with open('data/mensaje.txt', 'a') as f:
            f.write(mensaje)

        conn.close()
       
    except:
        print('No hay registros nuevos para cargar')
        
def enviar_email(**context):
    sender = context['var']['value'].get("from")
    receiver = context['var']['value'].get('to')
    password = context['var']['value'].get('password')
    fecha = date.today()
    meses = pd.Series(['Enero','Febrero','Marzo','Abril','Mayo','Junio','Julio','Agosto','Septiembre',
                      'Octubre', 'Noviembre', 'Diciembre'],
                       index = range(1,13,1))
    with open('data/mensaje.txt', 'r') as f:
        texto = f.read()
    
    msg = MIMEText(texto)
    msg['Subject'] = 'Carga Airflow - API BCRA: {} de {} de {}'.format(fecha.day, meses[fecha.month], fecha.year)
    msg['From'] = sender
    msg['To'] = receiver
    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp_server:
        smtp_server.login(sender, password)
        smtp_server.sendmail(sender, receiver, msg.as_string())
    print('Mensaje enviado')
    
default_args = {'owner': 'Mauro Rodríguez',
                'depends_on_past': True,
                'retries': 5,
                'retry_delay':timedelta(minutes=1)}
    
with DAG(dag_id = 'api_bcra',
         default_args = default_args,
         description = 'Carga datos en redshift desde la API del Banco Central de la República Argentina',
         start_date = datetime(2024,2,29),
         schedule_interval = '@daily',
         catchup = True) as dag:
    
    task1 = PythonOperator(task_id = 'descarga_info_api',
                           python_callable = conexion_api)
    task2 = PostgresOperator(task_id = 'crear_tabla_redshift',
                            postgres_conn_id = 'conn_redshift',
                            sql = 'sql/tabla_redshift.sql')
    task3 = PythonOperator(task_id = 'filtrar_registros_nvos',
                           python_callable = filtrar_registros)
    task4 = PythonOperator(task_id = 'carga_sql',
                           python_callable = conn_redshift)
    task5 = PythonOperator(task_id = 'enviar_email',
                          python_callable = enviar_email)
    
task1 >> task3 
task2 >> task4 
task3 >> task4 >> task5   
