"""DAG principal du lakehouse DPE.

Cadence hebdomadaire : l'ADEME rafraîchit la base une fois par semaine, un
ordonnancement quotidien ne ferait que recharger des partitions inchangées.

Enchaînement : ingestion incrémentale -> transformation Spark/Scala ->
chargement dans l'entrepôt -> modélisation dbt -> tests. Un échec de test dbt
fait échouer le DAG : mieux vaut un rapport Power BI figé sur les données de la
veille qu'un rapport actualisé avec des données fausses.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

# Nombre de mois rechargés à chaque exécution. Trois, et non un, parce que
# l'ADEME corrige des DPE a posteriori : un diagnostic établi en janvier peut
# être modifié en mars. Recharger une fenêtre glissante rattrape ces corrections
# sans imposer un rechargement complet.
MOIS_FENETRE_INCREMENTALE = 3

WAREHOUSE_HOST = os.getenv("WAREHOUSE_HOST", "warehouse")
WAREHOUSE_PORT = os.getenv("WAREHOUSE_PORT", "5432")
WAREHOUSE_DB = os.getenv("WAREHOUSE_DB", "dpe")
WAREHOUSE_USER = os.getenv("WAREHOUSE_USER", "dpe")
WAREHOUSE_PASSWORD = os.getenv("WAREHOUSE_PASSWORD", "dpe")

JDBC_URL = f"jdbc:postgresql://{WAREHOUSE_HOST}:{WAREHOUSE_PORT}/{WAREHOUSE_DB}"

# Le lac est visible sous deux chemins différents selon le conteneur :
# Airflow le monte sous /opt/airflow/data, Spark sous /opt/data. Les tâches
# d'ingestion s'exécutent dans Airflow, les jobs Spark dans le conteneur Spark,
# d'où deux racines distinctes plutôt qu'une seule.
LAKE_ROOT = os.getenv("DPE_BRONZE_ROOT", "/opt/airflow/data/bronze")
SPARK_DATA_ROOT = os.getenv("SPARK_DATA_ROOT", "/opt/data")
SPARK_BRONZE = f"{SPARK_DATA_ROOT}/bronze"
SPARK_SILVER = f"{SPARK_DATA_ROOT}/silver"

# Le jar est monté en lecture seule dans le conteneur Spark.
SPARK_JAR = "/opt/jars/dpe-spark-jobs.jar"

# Rouvre les paquets internes du JDK, encapsulés depuis le JDK 16 mais toujours
# atteints par réflexion par Spark dès qu'une colonne de type date est traitée.
SPARK_JDK_OPTS = " ".join(
    f"--add-opens=java.base/{module}=ALL-UNNAMED"
    for module in (
        "java.lang", "java.lang.invoke", "java.io", "java.net", "java.nio",
        "java.util", "java.util.concurrent", "sun.nio.ch", "sun.util.calendar",
    )
)

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    # Sans cette option, un rattrapage de plusieurs semaines lancerait autant
    # d'exécutions concurrentes se disputant la même API publique.
    "depends_on_past": False,
}

with DAG(
    dag_id="dpe_pipeline",
    description="Chaîne complète DPE ADEME : ingestion, Spark, entrepôt, dbt",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="0 4 * * 1",  # tous les lundis à 4 h
    catchup=False,
    max_active_runs=1,
    tags=["dpe", "ademe", "lakehouse"],
) as dag:

    debut = EmptyOperator(task_id="debut")

    # ------------------------------------------------------------------ #
    # 1. Ingestion incrémentale vers bronze
    # ------------------------------------------------------------------ #
    ingestion = BashOperator(
        task_id="ingestion_bronze",
        bash_command=(
            "cd /opt/airflow/ingestion && "
            "python -m dpe_ingest backfill "
            "--start $(date -d '{{ ds }} -%s months' +%%Y-%%m) "
            "--end {{ ds }} "
            "--force" % MOIS_FENETRE_INCREMENTALE
        ),
        env={
            "PYTHONPATH": "/opt/airflow/ingestion",
            "DPE_BRONZE_ROOT": LAKE_ROOT,
            "AWS_ENDPOINT_URL": os.getenv("AWS_ENDPOINT_URL", "http://minio:9000"),
            "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
            "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
        },
        append_env=True,
    )

    # ------------------------------------------------------------------ #
    # 2. Bronze -> silver (job Scala soumis au conteneur Spark)
    # ------------------------------------------------------------------ #
    transformation = BashOperator(
        task_id="spark_bronze_vers_silver",
        bash_command=(
            "docker exec dpe_spark spark-submit "
            "--class fr.dpelab.silver.DpeSilverJob "
            "--master 'local[*]' "
            "--driver-memory ${SPARK_DRIVER_MEMORY:-4g} "
            f"--conf 'spark.driver.extraJavaOptions={SPARK_JDK_OPTS}' "
            f"--conf 'spark.executor.extraJavaOptions={SPARK_JDK_OPTS}' "
            # Les temporaires de shuffle vont sur le volume du lac : la
            # déduplication de 15 M de lignes en produit plus de 10 Go, de quoi
            # saturer le disque système.
            f"--conf 'spark.local.dir={SPARK_DATA_ROOT}/tmp-spark' "
            f"{SPARK_JAR} "
            f"--bronze-path {SPARK_BRONZE}/dpe_existant "
            f"--silver-path {SPARK_SILVER}/dpe_courant "
            f"--rejects-path {SPARK_SILVER}/dpe_rejets "
            f"--metrics-path {SPARK_SILVER}/_metrics"
        ),
    )

    # ------------------------------------------------------------------ #
    # 3. Silver -> entrepôt PostgreSQL
    # ------------------------------------------------------------------ #
    chargement = BashOperator(
        task_id="chargement_entrepot",
        bash_command=(
            "docker exec dpe_spark spark-submit "
            "--class fr.dpelab.warehouse.LoadWarehouseJob "
            "--master 'local[*]' "
            "--driver-memory ${SPARK_DRIVER_MEMORY:-4g} "
            f"--conf 'spark.driver.extraJavaOptions={SPARK_JDK_OPTS}' "
            f"{SPARK_JAR} "
            f"--silver-path {SPARK_SILVER}/dpe_courant "
            f"--rejects-path {SPARK_SILVER}/dpe_rejets "
            f"--jdbc-url {JDBC_URL} "
            f"--user {WAREHOUSE_USER} "
            f"--password {WAREHOUSE_PASSWORD} "
            # Le lac garde l'historique complet depuis 2021 ; l'entrepôt ne
            # sert que la fenêtre analytique interrogée par les rapports.
            "--depuis-annee 2024"
        ),
    )

    # ------------------------------------------------------------------ #
    # 4. Modélisation dbt et tests
    # ------------------------------------------------------------------ #
    dbt_build = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/dbt/dpe_analytics && dbt run --profiles-dir .",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt/dpe_analytics && dbt test --profiles-dir .",
    )

    # La documentation est régénérée à chaque exécution : une doc de lignage
    # obsolète est pire qu'absente, parce qu'on lui fait confiance.
    dbt_docs = BashOperator(
        task_id="dbt_docs_generate",
        bash_command="cd /opt/airflow/dbt/dpe_analytics && dbt docs generate --profiles-dir .",
    )

    fin = EmptyOperator(task_id="fin")

    debut >> ingestion >> transformation >> chargement >> dbt_build >> dbt_test >> dbt_docs >> fin
