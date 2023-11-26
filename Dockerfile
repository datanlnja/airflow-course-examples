# Возьмем за основу данный образ
# Данная команда установит уже готвоый образ с python:3.8
# После чего поверх него установит дополнительные 
# команды которые мы укажем дальше
FROM python:3.8 

# Устанавливаем домашнюю директорию внутри контейнера
# При первом запуске Airflow в каталоге $AIRFLOW_HOME
# будет создан файл airflow.cfg.
ENV AIRFLOW_HOME=/usr/local/airflow

# Airflow глобальные переменные
ARG AIRFLOW_VERSION=2.1.4

# Здесь и выше мы использовали глобальные переменные
# Они нужны чтобы не прописывать каждый раз что то в 
# config файл, это проще и гибче, синтаксис следующий
# AIRFLOW__{SECTION}__{KEY} конкретные данные нужно искать в документации

# Папка с дагами и плагинами
ENV AIRFLOW__CORE__DAGS_FOLDER=/usr/local/airflow/dags 
ENV AIRFLOW__CORE__PLUGINS_FOLDER=/usr/local/airflow/plugins

# Замени executor и сменим бд на постгрес
ENV AIRFLOW__CORE__EXECUTOR=LocalExecutor
ENV AIRFLOW__CORE__SQL_ALCHEMY_CONN="postgresql+psycopg2://postgres:postgres@postgres:5432/airflow"

# Отключим примеры кода
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False

# Установка airflow с поддержкой всех баз данных
RUN pip install apache-airflow[postgres]==${AIRFLOW_VERSION}

# Установка визуального редактора для работы в Airflow UI
# Дополнительная настройка чтобы можно было редактировать код
# Внутри веб интерфейса
RUN pip install airflow-code-editor
RUN pip install black fs-s3fs fs-gcsfs

# Слздадим отдельную папку для нашего скрипта запуска
# Затем скопируем сам скрипт и после чего выдадим расширеные права
RUN mkdir /project
COPY scripts/ /project/scripts/
RUN chmod +x /project/scripts/init.sh

# Запускаем sh скрипт
# Начнет процесс инициализации airflow
ENTRYPOINT ["/project/scripts/init.sh"]