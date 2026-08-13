FROM apache/airflow:2.10.4-python3.11

USER root

# Le client Docker permet à Airflow de soumettre les jobs au conteneur Spark
# (`docker exec`) sans embarquer Spark lui-même dans l'image Airflow.
RUN apt-get update \
 && apt-get install -y --no-install-recommends docker.io curl \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

USER airflow

# dbt et les dépendances de l'ingestion, installés dans l'environnement Airflow
# pour que les tâches BashOperator y aient accès directement.
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
