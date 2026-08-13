"""Écriture de la couche bronze en Parquet, partitionnée et idempotente.

Garantie recherchée : relancer l'ingestion d'un mois déjà chargé doit produire
exactement le même état, sans doublon ni fichier à moitié écrit. C'est le point
qui sépare un script de démonstration d'un pipeline exploitable.

Mécanique : chaque partition est écrite dans un répertoire temporaire, puis
promue par un déplacement atomique. Un plantage en cours d'écriture laisse la
partition précédente intacte et le répertoire temporaire est purgé au run suivant.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterable
from dataclasses import dataclass, asdict
from datetime import datetime, timezone

import fsspec
import pyarrow as pa
import pyarrow.parquet as pq

from .config import IngestConfig

log = logging.getLogger(__name__)


@dataclass
class PartitionManifest:
    """Trace ce qui a réellement été écrit, pour audit et contrôle de complétude."""

    partition: str
    rows_written: int
    rows_announced_by_source: int
    ingested_at_utc: str
    source_filter: str
    files: list[str]

    @property
    def is_complete(self) -> bool:
        return self.rows_written == self.rows_announced_by_source


class BronzeSink:
    def __init__(self, config: IngestConfig) -> None:
        self.config = config
        self.root = config.bronze_root.rstrip("/")
        self.fs, _ = fsspec.core.url_to_fs(self.root, **config.storage_options())

    # ------------------------------------------------------------------ #
    # Chemins
    # ------------------------------------------------------------------ #
    def _partition_path(self, table: str, year: int, month: int) -> str:
        return f"{self.root}/{table}/annee={year:04d}/mois={month:02d}"

    def _staging_path(self, table: str, year: int, month: int) -> str:
        return f"{self.root}/{table}/_staging/annee={year:04d}_mois={month:02d}"

    def _manifest_path(self, table: str, year: int, month: int) -> str:
        return f"{self._partition_path(table, year, month)}/_manifest.json"

    # ------------------------------------------------------------------ #
    # État
    # ------------------------------------------------------------------ #
    def read_manifest(self, table: str, year: int, month: int) -> PartitionManifest | None:
        path = self._manifest_path(table, year, month)
        if not self.fs.exists(path):
            return None
        try:
            with self.fs.open(path, "rb") as handle:
                return PartitionManifest(**json.loads(handle.read().decode("utf-8")))
        except (json.JSONDecodeError, TypeError) as exc:
            log.warning("Manifeste illisible pour %s (%s) — partition à recharger", path, exc)
            return None

    def is_partition_complete(self, table: str, year: int, month: int) -> bool:
        """Une partition est réputée chargée si son manifeste est présent ET complet."""
        manifest = self.read_manifest(table, year, month)
        return manifest is not None and manifest.is_complete

    # ------------------------------------------------------------------ #
    # Écriture
    # ------------------------------------------------------------------ #
    def write_partition(
        self,
        table: str,
        year: int,
        month: int,
        batches: Iterable[pa.Table],
        rows_announced: int,
        source_filter: str,
    ) -> PartitionManifest:
        """Écrit une partition complète de façon atomique.

        `batches` est consommé en flux : chaque page téléchargée part sur disque
        immédiatement, ce qui garde l'empreinte mémoire constante quelle que
        soit la taille du mois (certains dépassent 400 000 lignes).
        """
        staging = self._staging_path(table, year, month)
        final = self._partition_path(table, year, month)

        # Purge d'un éventuel résidu d'un run interrompu.
        self._remove(staging)
        self.fs.makedirs(staging, exist_ok=True)

        rows_written = 0
        written_files: list[str] = []

        for index, batch in enumerate(batches):
            if batch.num_rows == 0:
                continue
            filename = f"part-{index:05d}.parquet"
            target = f"{staging}/{filename}"
            with self.fs.open(target, "wb") as handle:
                pq.write_table(batch, handle, compression="snappy")
            rows_written += batch.num_rows
            written_files.append(filename)

        manifest = PartitionManifest(
            partition=f"annee={year:04d}/mois={month:02d}",
            rows_written=rows_written,
            rows_announced_by_source=rows_announced,
            ingested_at_utc=datetime.now(timezone.utc).isoformat(),
            source_filter=source_filter,
            files=written_files,
        )

        with self.fs.open(f"{staging}/_manifest.json", "wb") as handle:
            handle.write(json.dumps(asdict(manifest), indent=2).encode("utf-8"))

        # Promotion atomique : l'ancienne partition ne disparaît qu'une fois la
        # nouvelle intégralement écrite.
        self._remove(final)
        self._ensure_parent(final)
        self.fs.mv(staging, final, recursive=True)

        log.info(
            "Partition %s écrite : %d lignes (source en annonce %d)%s",
            manifest.partition,
            rows_written,
            rows_announced,
            "" if manifest.is_complete else "  <-- ECART DETECTE",
        )
        return manifest

    # ------------------------------------------------------------------ #
    # Helpers
    # ------------------------------------------------------------------ #
    def _remove(self, path: str) -> None:
        if self.fs.exists(path):
            try:
                self.fs.rm(path, recursive=True)
            except FileNotFoundError:
                pass

    def _ensure_parent(self, path: str) -> None:
        parent = path.rsplit("/", 1)[0]
        self.fs.makedirs(parent, exist_ok=True)

    def cleanup_staging(self, table: str) -> None:
        """Supprime les répertoires temporaires laissés par des runs interrompus."""
        staging_root = f"{self.root}/{table}/_staging"
        if self.fs.exists(staging_root):
            log.info("Purge des partitions temporaires sous %s", staging_root)
            self._remove(staging_root)
