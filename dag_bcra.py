import pandas as pd
import requests 
import json
import time
from datetime import datetime, timedelta
from configparser import ConfigParser
import sqlalchemy as sa
from airflow import DAG
from airflow.operators.python import PythonOperator
from email.mime.text import MIMEText
import smtplib

def conexion_api():
    
    parser = ConfigParser()
    parser.read('credenciales/credenciales.ini')
    config = parser['redshift']
    
    def bcra(codigo, nombre_variable, credenciales):
        headers = dict(Authorization = credenciales['api_token'])
        api = 'https://api.estadisticasbcra.com/{}'.format(codigo)
        data = requests.get(api, headers = headers).json()
        data = pd.DataFrame(data)
        data.columns = ['fecha', nombre_variable]
        data['fecha'] = pd.to_datetime(data['fecha'], format = '%Y-%m-%d')
        return data
    
    data = bcra('usd_of', 'dolar_oficial', config)
    
    códigos = ['usd', 'var_usd_vs_usd_of','base', 'reservas', 'base_div_res','circulacion_monetaria', 'depositos', 
       'cuentas_corrientes', 'tasa_adelantos_cuenta_corriente', 'cajas_ahorro', 'plazo_fijo', 'tasa_depositos_30_dias',
        'prestamos', 'tasa_prestamos_personales', 'porc_prestamos_vs_depositos','merval', 'merval_usd']
    
    nombres_variables = ['dolar_paralelo', 'brecha_dolar_oficial_paralelo', 'base_monetaria', 'reservas_internacionales',
                    'base_monetaria_dividida_reservas', 'circulacion_monetaria', 'depositos',
                    'cuentas_corrientes', 'tasa_adelantos_cuenta_corriente', 'cajas_ahorro', 'plazos_fijos',
                    'tasa_depositos_30_dias', 'prestamos', 'tasa_prestamos_personales', 'porc_prestamos_vs_depositos',
                         'merval', 'merval_usd']
    
    for i in range(len(códigos)):
        data2 = bcra(códigos[i], nombres_variables[i], config)
        data = pd.merge(data, data2, how= 'outer')
        data = data.sort_values('fecha')
        time.sleep(1)
        
    data.drop_duplicates(inplace= True)
    data.dropna(inplace=True)
    
    data.to_csv('data/datos_bcra.csv', index= False)
    
    print('La conexión a la API fue exitosa')

def filtrar_registros():

    parser = ConfigParser()
    parser.read('credenciales/credenciales.ini')
    config = parser['redshift']
    host = config['host']
    port = config['port']
    dbname = config['dbname']
    username = config['username']
    pwd = config['pwd']
    
    conn_string =f'postgresql://{username}:{pwd}@{host}:{port}/{dbname}?sslmode=require'

    engine = sa.create_engine(conn_string)
    
    conn = engine.connect()
    
    ultima_fecha = pd.read_sql('select max(fecha) from rodriguez_mauro11_coderhouse.bcra', conn)
    
    data = pd.read_csv('data/datos_bcra.csv')
    
    print('ultima carga: {}'.format(ultima_fecha['max'].iloc[0]))
    
    if ultima_fecha['max'].iloc[0] == None:
        print("No hay datos en redshift. Se insertarán los {} registros existentes de la API".format(len(data)))
        
        conn.close()

    else:
        
        ultima_fecha = str(ultima_fecha['max'][0])
        data = data[data['fecha'] > ultima_fecha]

        if len(data) != 0:
            
            data.to_csv('data/datos_bcra.csv', index = False)
            
            print("Existen {} registro/s nuevo/s que será/n cargardo/s en redshift".format(len(data)))
          
            conn.close()
        else:
            print('No hay registros nuevos para cargar en redshift.')
            data = pd.DataFrame()
            data.to_csv('data/datos_bcra.csv', index = False)
            conn.close()
    
    
def conn_redshift():
    
    try:

        data = pd.read_csv('data/datos_bcra.csv')
    
        parser = ConfigParser()
        parser.read('credenciales/credenciales.ini')
        config = parser['redshift']
        host = config['host']
        port = config['port']
        dbname = config['dbname']
        username = config['username']
        pwd = config['pwd']
    
        conn_string =f'postgresql://{username}:{pwd}@{host}:{port}/{dbname}?sslmode=require'

        engine = sa.create_engine(conn_string)
    
        conn = engine.connect()

        data.to_sql(name= 'bcra', con = conn, if_exists= 'append', method= 'multi', 
           chunksize= 1000, index= False)

        print('Fueron cargados {} registro/s en Amazon Redsfhit'.format(len(data)))

        conn.close()
       
    except:
        print('No hay registros nuevos para cargar')
        
def enviar_email():
    parser = ConfigParser()
    parser.read('credenciales/credenciales.ini')
    config = parser['e-mail']
    
    msg = MIMEText('texto')
    msg['Subject'] = 'Título del mail'
    msg['From'] = config['from_user']
    msg['To'] = config['to_user']
    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp_server:
        smtp_server.login(config['from_user'], config['password'])
        smtp_server.sendmail(config['from_user'], config['to_user'], msg.as_string())
    print('Mensaje enviado')

# Con variables creadas en el webserver

def enviar_email2(**context):
    sender = context['var']['value'].get("EMAIL_SENDER_MJ")
    recipients = [context['var']['value'].get('EMAIL_RECEIVER_MJ')] #lista por si hay varios destinatarios
    password = context['var']['value'].get('EMAIL_PASSWORD_CODER') 
#Los gets apuntan a los nombres de las variables creadas.

Gestionar cuenta de google >> Seguridad >> contraseña de aplicación

Podemos cambiar la sección de SMTP de airflow.cfg y loguear nuestro email. Luego podemos usar el siguiente operador donde solo hay que poner los destinatarios y el mensaje.
from airflow.operators.email_operator import EmailOperator
    
default_args = {'owner': 'Mauro Rodríguez',
                'depends_on_past': True,
                'retries': 0,
                'retry_delay':timedelta(minutes=1)}
    
with DAG(dag_id = 'api_bcra',
         default_args = default_args,
         description = 'Carga datos en redshift desde la API del Banco Central de la República Argentina',
         start_date = datetime(2024,2,24),
         schedule_interval = '@daily',
         catchup = True) as dag:
    
    task1 = PythonOperator(task_id = 'descarga_info_api',
                           python_callable = conexion_api)
    task2 = PythonOperator(task_id = 'filtrar_registros_nvos',
                           python_callable = filtrar_registros)
    task3 = PythonOperator(task_id = 'carga_sql',
                           python_callable = conn_redshift)
    task4 = PythonOperator(task_id = 'enviar e-mail',
                          python_callable = enviar_email)
    
task1 >> task2 >> task3 >> task4    
