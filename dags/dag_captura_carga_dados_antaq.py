from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.email_operator import EmailOperator

from functions.captura_dados_atracacao import captura_dados_atracacao
from functions.captura_dados_carga import captura_dados_carga

from functions.extrai_dados_atracacao import extrai_dados_atracacao
from functions.extrai_dados_carga import extrai_dados_carga

from functions.transforma_dados import transforma_dados

from functions.carrega_dados_atracacao import carrega_dados_atracacao
from functions.carrega_dados_carga import carrega_dados_carga

email = 'lucas.belmino15@gmail.com'

default_args = {
    'owner': 'airflow',
    'email': email,
    'email_on_failure': True
    }

dag = DAG(
    'captura_carga_dados_antaq',
    default_args=default_args,
    start_date=datetime(2023, 4, 5),
    schedule_interval='@monthly',
    description='DAG do Airflow para Captura, Transformacao e Carga de dados da Antaq'
    )

start_task = EmptyOperator(
        task_id='start'
        )

captura_dados_atracacao_task = PythonOperator(
    task_id='captura_dados_atracao',
    python_callable=captura_dados_atracacao,
    dag=dag
    )

captura_dados_carga_task = PythonOperator(
    task_id='captura_dados_carga',
    python_callable=captura_dados_carga,
    dag=dag
    )

extrai_dados_atracacao_task = PythonOperator(
    task_id='extrai_dados_atracacao',
    python_callable=extrai_dados_atracacao,
    dag=dag
    )

extrai_dados_carga_task = PythonOperator(
    task_id='extrai_dados_carga',
    python_callable=extrai_dados_carga,
    dag=dag
    )

transforma_dados_task = PythonOperator(
    task_id='transforma_dados',
    python_callable=transforma_dados,
    dag=dag
    )

carrega_dados_atracacao_task = PythonOperator(
    task_id='carrega_dados_atracacao',
    python_callable=carrega_dados_atracacao,
    dag=dag
    )

carrega_dados_carga_task = PythonOperator(
    task_id='carrega_dados_carga',
    python_callable=carrega_dados_carga,
    dag=dag
    )

envia_email_conclusao_task = EmailOperator(
        task_id='send_email',
        subject='Airflow Alert',
        to=email,
        html_content=""" <h3>Air flow Sucessed</h3> """,
        dag=dag
)

start_task >> [captura_dados_atracacao_task, captura_dados_carga_task]
captura_dados_atracacao_task >> extrai_dados_atracacao_task
captura_dados_carga_task >> extrai_dados_carga_task
[extrai_dados_atracacao_task, extrai_dados_carga_task] >> transforma_dados_task
transforma_dados_task >> [carrega_dados_atracacao_task, carrega_dados_carga_task]
[carrega_dados_atracacao_task, carrega_dados_carga_task] >> envia_email_conclusao_task