import pandas as pd
import requests 
import json
import time
from datetime import date
import sqlalchemy as sa
from airflow.providers.postgres.hooks.postgres import PostgresHook
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
