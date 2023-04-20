from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.empty import EmptyOperator

from functions.captura_dados_atracacao import captura_dados_atracacao
from functions.captura_dados_carga import captura_dados_carga

def extrai_dados_atracacao():
    # Insira aqui o código para extrair dados dos arquivos capturados
    print('Dados extraídos')

def extrai_dados_carga():
    # Insira aqui o código para extrair dados dos arquivos capturados
    print('Dados extraídos')

def verifica_captura():
    # Insira aqui o código para verificar se os dados foram capturados corretamente
    print('Dados verificados')

def transforma_dados_atracacao():
    # Insira aqui o código para transformar os dados de atração
    print('Dados de atração transformados')

def transforma_dados_carga():
    # Insira aqui o código para transformar os dados de carga
    print('Dados de carga transformados')

def carrega_dados_atracacao():
    # Insira aqui o código para carregar os dados de atração em um banco de dados
    print('Dados de atração carregados')

def carrega_dados_carga():
    # Insira aqui o código para carregar os dados de carga em um banco de dados
    print('Dados de carga carregados')

def envia_email_conclusao():
    # Insira aqui o código para enviar um email informando que a DAG foi concluída
    print('Email de conclusão enviado')

default_args = {
    'owner': 'airflow'
    }

dag = DAG(
    'captura_carga_dados_antaq',
    default_args=default_args,
    start_date=datetime(2022, 5, 28),
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