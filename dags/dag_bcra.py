import pandas as pd
import requests 
import json
import time
from datetime import datetime, timedelta
from configparser import ConfigParser
import sqlalchemy as sa
from airflow import DAG
from airflow.operators.python import PythonOperator

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
    
    print('ultima fecha cargada: {}'.format(ultima_fecha['max'].iloc[0]))
    
    if ultima_fecha['max'].iloc[0] == None:
        print('No hay datos en redshift. Hay que insertar todos los registros de la API.')
        conn.close()
    else:
        data = pd.read_csv('data/datos_bcra.csv')
        ultima_fecha = str(ultima_fecha['max'][0])
        data = data[data['fecha'] > ultima_fecha]

        if len(data) != 0:
            
            data.to_csv('data/datos_bcra.csv', index = False)
            
            print('Los registros nuevos fueron filtrados.')
          
            conn.close()
        else:
            print('No hay registros nuevos para cargar.')
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

        print('registros nuevos cargados')

        conn.close()
       
    except:
        print('No se cargaron registros')
        
            
default_args = {'owner': 'Mauro Rodríguez',
                'depends_on_past': True,
                'retries': 0,
                'retry_delay':timedelta(minutes=1)}
    
with DAG(dag_id = 'api_bcra',
         default_args = default_args,
         description = 'Carga datos en redshift desde la API del Banco Central de la República Argentina',
         start_date = datetime(2024,2,23),
         schedule_interval = '0 23 * * *',
         catchup = False) as dag:
    
    task1 = PythonOperator(task_id = 'descarga_info_api',
                           python_callable = conexion_api)
    task2 = PythonOperator(task_id = 'filtrar_registros_nvos',
                           python_callable = filtrar_registros)
    task3 = PythonOperator(task_id = 'carga_sql',
                           python_callable = conn_redshift)
    
task1 >> task2 >> task3    
