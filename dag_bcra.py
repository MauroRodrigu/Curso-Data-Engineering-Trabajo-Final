from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from funciones import conexion_api, filtrar_registros, conn_redshift, enviar_email
from datetime import timedelta, datetime

default_args = {'owner': 'Mauro Rodríguez',
                'depends_on_past': True,
                'retries': 5,
                'retry_delay':timedelta(minutes=1)}
    
with DAG(dag_id = 'api_bcra',
         default_args = default_args,
         description = 'Carga datos en redshift desde la API del Banco Central de la República Argentina',
         start_date = datetime(2024,3,7),
         schedule_interval = '@daily',
         catchup = False) as dag:
    
    task1 = PythonOperator(task_id = 'descarga_info_api',
                           python_callable = conexion_api)
    task2 = PostgresOperator(task_id = 'crear_tabla_redshift',
                            postgres_conn_id = 'conn_redshift',
                            sql = 'tabla_redshift.sql')
    task3 = PythonOperator(task_id = 'filtrar_registros_nvos',
                           python_callable = filtrar_registros)
    task4 = PythonOperator(task_id = 'carga_sql',
                           python_callable = conn_redshift)
    task5 = PythonOperator(task_id = 'enviar_email',
                          python_callable = enviar_email)
    
task1 >> task3 
task2 >> task4 
task3 >> task4 >> task5   
