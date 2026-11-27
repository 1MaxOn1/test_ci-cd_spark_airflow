FROM apache/airflow:3.1.3-python3.10

WORKDIR /opt/airflow

USER root

RUN apt update && apt -y install procps default-jre

USER airflow
COPY ./dags/* ./dags
COPY ./spark ./spark

RUN pip3 install apache-airflow-providers-apache-spark==5.4.1 \
     && pip3 install pyspark==4.0.1
     
COPY /custom_dag/mall_customers_working.py /opt/dags