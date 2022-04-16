#FROM python:3.6
#FROM puckel/docker-airflow
FROM apache/airflow:2.2.5

RUN pip install --upgrade pip


COPY ./util ./
# COPY ./dags /airflow/dags
COPY requirements.txt ./requirements.txt
RUN cat /etc/os-release
RUN pip install -r requirements.txt

USER root
RUN apt-get update
RUN apt-get install -y wget

RUN apt-get install -y software-properties-common
RUN apt-add-repository -y 'deb http://security.debian.org/debian-security stretch/updates main'
RUN apt-get update
RUN apt-get install -y openjdk-8-jdk
#RUN apt-get install openjdk-8-jdk-headless 
RUN wget https://download.microsoft.com/download/0/2/A/02AAE597-3865-456C-AE7F-613F99F850A8/sqljdbc_6.0.8112.200_enu.tar.gz
RUN tar -xf sqljdbc_6.0.8112.200_enu.tar.gz
RUN mv sqljdbc_6.0 /opt/