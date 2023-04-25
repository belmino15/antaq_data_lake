FROM apache/airflow:2.5.3

ADD requirements.txt .

COPY dags /opt/airflow/dags

RUN pip install -r requirements.txt