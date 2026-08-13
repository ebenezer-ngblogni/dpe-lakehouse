"""Configuration de l'ingestion, pilotée par variables d'environnement.

Aucune valeur secrète n'est codée en dur : le fichier `.env.example` documente
les clés attendues, et docker-compose les injecte dans les conteneurs.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import date

# Le jeu "DPE logements existants" démarre au 1er juillet 2021 (entrée en vigueur
# du DPE réformé). Rien avant cette date dans la source.
DATASET_START = date(2021, 7, 1)

API_BASE = "https://data.ademe.fr/data-fair/api/v1/datasets"
DATASET_ID = "dpe03existant"


@dataclass(frozen=True)
class IngestConfig:
    """Paramètres d'un run d'ingestion."""

    # --- Source ---
    api_base: str = field(default_factory=lambda: os.getenv("DPE_API_BASE", API_BASE))
    dataset_id: str = field(default_factory=lambda: os.getenv("DPE_DATASET_ID", DATASET_ID))
    page_size: int = field(default_factory=lambda: int(os.getenv("DPE_PAGE_SIZE", "10000")))

    # --- Destination ---
    # Accepte un chemin local ("data/bronze") ou une URI S3/MinIO ("s3://lake/bronze").
    bronze_root: str = field(default_factory=lambda: os.getenv("DPE_BRONZE_ROOT", "data/bronze"))
    s3_endpoint: str | None = field(default_factory=lambda: os.getenv("AWS_ENDPOINT_URL") or None)
    s3_key: str | None = field(default_factory=lambda: os.getenv("AWS_ACCESS_KEY_ID") or None)
    s3_secret: str | None = field(default_factory=lambda: os.getenv("AWS_SECRET_ACCESS_KEY") or None)

    # --- Robustesse réseau ---
    max_retries: int = field(default_factory=lambda: int(os.getenv("DPE_MAX_RETRIES", "5")))
    backoff_base: float = field(default_factory=lambda: float(os.getenv("DPE_BACKOFF_BASE", "1.5")))
    timeout_seconds: float = field(default_factory=lambda: float(os.getenv("DPE_TIMEOUT", "120")))

    # Politesse vis-à-vis d'une API publique gratuite : plafonne le débit.
    sleep_between_pages: float = field(
        default_factory=lambda: float(os.getenv("DPE_SLEEP_BETWEEN_PAGES", "0.2"))
    )

    @property
    def lines_url(self) -> str:
        return f"{self.api_base}/{self.dataset_id}/lines"

    @property
    def dataset_url(self) -> str:
        return f"{self.api_base}/{self.dataset_id}"

    @property
    def is_s3(self) -> bool:
        return self.bronze_root.startswith("s3://")

    def storage_options(self) -> dict:
        """Options fsspec pour l'écriture Parquet (vide si destination locale)."""
        if not self.is_s3:
            return {}
        opts: dict = {}
        if self.s3_endpoint:
            opts["endpoint_url"] = self.s3_endpoint
        if self.s3_key:
            opts["key"] = self.s3_key
        if self.s3_secret:
            opts["secret"] = self.s3_secret
        return opts
