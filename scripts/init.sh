#!/bin/bash

# Создание БД
sleep 10
airflow db init
sleep 10

airflow users create \
          --username admin \
          --firstname admin \
          --lastname admin \
          --role Admin \
          --email admin@example.org \
          -p 12345

# Запуск шедулера и вебсервера
airflow scheduler & airflow webserver