#!/bin/bash

# Создание БД
airflow db init
sleep 15

airflow users create \
          --username airflow \
          --firstname airflow \
          --lastname airflow \
          --role Admin \
          --email admin@example.org \
          -p djfw938hrjq9

# Запуск шедулера и вебсервера
airflow scheduler & airflow webserver