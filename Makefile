.DEFAULT_GOAL := help
SHELL := /bin/bash

VENV        := .venv
PYTHON      := $(VENV)/bin/python
PIP         := $(VENV)/bin/pip
SBT         := PATH="$$HOME/.local/bin:$$PATH" sbt
JAR         := spark-jobs/target/scala-2.12/dpe-spark-jobs.jar
BRONZE      := data/bronze/dpe_existant
SILVER      := data/silver

# Runtime Spark : celui du dépôt s'il a été téléchargé (`make spark-runtime`),
# sinon celui du PATH.
SPARK_HOME  ?= $(wildcard tools/spark-3.5.3-bin-hadoop3)
SPARK_SUBMIT = $(if $(SPARK_HOME),$(SPARK_HOME)/bin/spark-submit,spark-submit)

# Répertoire des fichiers temporaires de Spark (shuffle, spill).
# La déduplication de 15,3 M de lignes brasse plus de 10 Go de shuffle : par
# défaut ces fichiers vont dans /tmp et saturent le disque système. Ce sont des
# temporaires purs, sans besoin de permissions POSIX, donc n'importe quel disque
# convient — y compris NTFS.
SPARK_LOCAL_DIR ?= $(if $(wildcard data/.),$(shell readlink -f data)/tmp-spark,/tmp/spark-dpe)

# Spark accède à des paquets internes du JDK par réflexion ; depuis le JDK 16
# il faut les rouvrir explicitement, sinon le job échoue dès la première
# manipulation de date.
SPARK_JDK_OPTS := \
  --add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
  --add-opens=java.base/java.io=ALL-UNNAMED \
  --add-opens=java.base/java.net=ALL-UNNAMED \
  --add-opens=java.base/java.nio=ALL-UNNAMED \
  --add-opens=java.base/java.util=ALL-UNNAMED \
  --add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
  --add-opens=java.base/sun.util.calendar=ALL-UNNAMED

.PHONY: help
help: ## Affiche cette aide
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'

# ---------------------------------------------------------------- #
# Environnement
# ---------------------------------------------------------------- #
.PHONY: setup
setup: ## Crée l'environnement Python et installe les dépendances
	python3 -m venv $(VENV)
	$(PIP) install --quiet --upgrade pip
	$(PIP) install --quiet -r ingestion/requirements-dev.txt
	@echo "Environnement prêt."

# ---------------------------------------------------------------- #
# Ingestion
# ---------------------------------------------------------------- #
.PHONY: ingest-sample
ingest-sample: ## Charge un seul mois (bac à sable, ~2 min)
	PYTHONPATH=ingestion DPE_BRONZE_ROOT=data/bronze \
		$(PYTHON) -u -m dpe_ingest backfill --start 2021-07 --end 2021-07

.PHONY: ingest-full
ingest-full: ## Charge l'historique complet depuis juillet 2021 (~5 h, reprenable)
	PYTHONPATH=ingestion DPE_BRONZE_ROOT=data/bronze \
		$(PYTHON) -u -m dpe_ingest backfill

.PHONY: ingest-status
ingest-status: ## Compare l'état local à la source, partition par partition
	PYTHONPATH=ingestion DPE_BRONZE_ROOT=data/bronze \
		$(PYTHON) -m dpe_ingest status

# ---------------------------------------------------------------- #
# Spark / Scala
# ---------------------------------------------------------------- #
.PHONY: build
build: ## Compile le jar Spark
	cd spark-jobs && $(SBT) -batch assembly

.PHONY: test-scala
test-scala: ## Exécute les tests Scala
	cd spark-jobs && $(SBT) -batch test

.PHONY: test-python
test-python: ## Exécute les tests Python
	PYTHONPATH=ingestion $(VENV)/bin/pytest ingestion -q

.PHONY: test
test: test-scala test-python ## Exécute toute la suite de tests

.PHONY: lint
lint: ## Vérifie le style Python
	$(VENV)/bin/ruff check ingestion

.PHONY: spark-runtime
spark-runtime: ## Télécharge le runtime Spark local (~400 Mo)
	mkdir -p tools && cd tools && \
	curl -fL -o spark.tgz https://archive.apache.org/dist/spark/spark-3.5.3/spark-3.5.3-bin-hadoop3.tgz && \
	tar xzf spark.tgz && rm spark.tgz

.PHONY: silver
silver: $(JAR) ## Lance la transformation bronze -> silver en local
	@mkdir -p "$(SPARK_LOCAL_DIR)"
	@echo "Temporaires Spark : $(SPARK_LOCAL_DIR)"
	$(SPARK_SUBMIT) --class fr.dpelab.silver.DpeSilverJob --master 'local[*]' \
		--driver-memory $${SPARK_DRIVER_MEMORY:-5g} \
		--conf "spark.local.dir=$(SPARK_LOCAL_DIR)" \
		--conf "spark.driver.extraJavaOptions=$(SPARK_JDK_OPTS)" \
		--conf "spark.executor.extraJavaOptions=$(SPARK_JDK_OPTS)" \
		$(JAR) \
		--bronze-path $(BRONZE) --silver-path $(SILVER)/dpe_courant \
		--rejects-path $(SILVER)/dpe_rejets --metrics-path $(SILVER)/_metrics \
		--shuffle-partitions 200

.PHONY: warehouse
warehouse: $(JAR) ## Charge silver vers PostgreSQL (fenêtre depuis 2024)
	$(SPARK_SUBMIT) --class fr.dpelab.warehouse.LoadWarehouseJob --master 'local[*]' \
		--driver-memory $${SPARK_DRIVER_MEMORY:-5g} \
		--packages org.postgresql:postgresql:42.7.4 \
		--conf "spark.driver.extraJavaOptions=$(SPARK_JDK_OPTS)" \
		$(JAR) \
		--silver-path $(SILVER)/dpe_courant --rejects-path $(SILVER)/dpe_rejets \
		--jdbc-url jdbc:postgresql://localhost:5434/dpe \
		--depuis-annee 2024

# ---------------------------------------------------------------- #
# Infrastructure
# ---------------------------------------------------------------- #
.PHONY: up
up: ## Démarre la pile Docker complète
	docker compose up -d
	@echo "MinIO    : http://localhost:9003"
	@echo "Airflow  : http://localhost:8080  (admin / admin)"
	@echo "Entrepôt : postgresql://localhost:5434/dpe"

.PHONY: down
down: ## Arrête la pile Docker
	docker compose down

.PHONY: clean
clean: ## Supprime les artefacts de build (conserve les données)
	cd spark-jobs && $(SBT) -batch clean
	rm -rf dbt/dpe_analytics/target dbt/dpe_analytics/logs

# ---------------------------------------------------------------- #
# dbt
# ---------------------------------------------------------------- #
.PHONY: dbt-run
dbt-run: ## Construit les modèles dbt
	cd dbt/dpe_analytics && dbt run --profiles-dir .

.PHONY: dbt-test
dbt-test: ## Exécute les tests dbt
	cd dbt/dpe_analytics && dbt test --profiles-dir .

.PHONY: dbt-docs
dbt-docs: ## Génère et sert la documentation dbt (lignage)
	cd dbt/dpe_analytics && dbt docs generate --profiles-dir . && dbt docs serve --profiles-dir .

# ---------------------------------------------------------------- #
# Restitution
# ---------------------------------------------------------------- #
.PHONY: powerbi-export
powerbi-export: ## Exporte les marts en CSV/Parquet pour Power BI Desktop
	$(PYTHON) scripts/export_powerbi.py

$(JAR):
	$(MAKE) build
