.DEFAULT_GOAL := help
SHELL := /bin/bash

VENV        := .venv
PYTHON      := $(VENV)/bin/python
PIP         := $(VENV)/bin/pip
SBT         := PATH="$$HOME/.local/bin:$$PATH" sbt
JAR         := spark-jobs/target/scala-2.12/dpe-spark-jobs.jar
BRONZE      := data/bronze/dpe_existant
SILVER      := data/silver

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

.PHONY: silver
silver: $(JAR) ## Lance la transformation bronze -> silver en local
	spark-submit --class fr.dpelab.silver.DpeSilverJob --master 'local[*]' \
		--driver-memory 4g $(JAR) \
		--bronze-path $(BRONZE) --silver-path $(SILVER)/dpe_courant \
		--rejects-path $(SILVER)/dpe_rejets --metrics-path $(SILVER)/_metrics

# ---------------------------------------------------------------- #
# Infrastructure
# ---------------------------------------------------------------- #
.PHONY: up
up: ## Démarre la pile Docker complète
	docker compose up -d
	@echo "MinIO    : http://localhost:9001"
	@echo "Airflow  : http://localhost:8080  (admin / admin)"
	@echo "Entrepôt : postgresql://localhost:5433/dpe"

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

$(JAR):
	$(MAKE) build
