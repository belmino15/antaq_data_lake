from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.email_operator import EmailOperator

from functions.captura_dados_atracacao import captura_dados_atracacao
from functions.captura_dados_carga import captura_dados_carga

from functions.extrai_dados_atracacao import extrai_dados_atracacao
from functions.extrai_dados_carga import extrai_dados_carga

from functions.transforma_dados_atracacao import transforma_dados_atracacao
from functions.transforma_dados_carga import transforma_dados_carga

from functions.carrega_dados_atracacao import carrega_dados_atracacao
from functions.carrega_dados_carga import carrega_dados_carga

email = 'lucas.belmino15@gmail.com'

def verifica_captura():
    # Insira aqui o código para verificar se os dados foram capturados corretamente
    print('Dados verificados')

def envia_email_conclusao():
    # Insira aqui o código para enviar um email informando que a DAG foi concluída
    print('Email de conclusão enviado')

default_args = {
    'owner': 'airflow'
    }

dag = DAG(
    'captura_carga_dados_antaq',
    default_args=default_args,
    start_date=datetime(2023, 4, 20),
    schedule_interval=None,
    description='DAG do Airflow para Captura, Transformacao e Carga de dados da Antaq'
    )

start_task = EmptyOperator(
        task_id='start'
        )

captura_dados_atracacao = PythonOperator(
    task_id='captura_dados_atracao',
    python_callable=captura_dados_atracacao,
    dag=dag
    )

captura_dados_carga = PythonOperator(
    task_id='captura_dados_carga',
    python_callable=captura_dados_carga,
    dag=dag
    )

verifica_captura = PythonOperator(
    task_id='verifica_captura',
    python_callable=verifica_captura,
    dag=dag
    )

extrai_dados_atracacao = PythonOperator(
    task_id='extrai_dados_atracacao',
    python_callable=extrai_dados_atracacao,
    dag=dag
    )

extrai_dados_carga = PythonOperator(
    task_id='extrai_dados_carga',
    python_callable=extrai_dados_carga,
    dag=dag
    )

transforma_dados_atracacao = PythonOperator(
    task_id='transforma_dados_atracacao',
    python_callable=transforma_dados_atracacao,
    dag=dag
    )

transforma_dados_carga = PythonOperator(
    task_id='transforma_dados_carga',
    python_callable=transforma_dados_carga,
    dag=dag
    )

carrega_dados_atracacao = PythonOperator(
    task_id='carrega_dados_atracacao',
    python_callable=carrega_dados_atracacao,
    dag=dag
    )

carrega_dados_carga = PythonOperator(
    task_id='carrega_dados_carga',
    python_callable=carrega_dados_carga,
    dag=dag
    )

envia_email_conclusao = PythonOperator(
    task_id='envia_email_conclusao',
    python_callable=envia_email_conclusao,
    dag=dag
    )

start_task >> [captura_dados_atracacao, captura_dados_carga]
captura_dados_atracacao >> extrai_dados_atracacao
captura_dados_carga >> extrai_dados_carga
[extrai_dados_atracacao, extrai_dados_carga] >> verifica_captura >> [transforma_dados_atracacao, transforma_dados_carga]
transforma_dados_atracacao >> carrega_dados_atracacao
transforma_dados_carga >> carrega_dados_carga
[carrega_dados_atracacao, carrega_dados_carga] >> envia_email_conclusao