#!/bin/bash
# Instala dependências
pip install --user -r /requirements.txt

# Inicializa banco e cria usuário (só uma vez)
airflow db init
airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com

# Inicia webserver e scheduler
airflow webserver &
airflow scheduler