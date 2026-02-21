FROM apache/airflow:2.7.1-python3.11

USER root
RUN apt-get update && \
    apt-get install -y gcc python3-dev openjdk-11-jdk procps curl && \
    apt-get clean

RUN curl -L https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz \
    | tar -xz -C /opt/ && \
    mv /opt/spark-3.5.0-bin-hadoop3 /opt/spark

ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:/opt/spark/bin
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

USER airflow

RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark==4.1.4 \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.7.1/constraints-3.11.txt"