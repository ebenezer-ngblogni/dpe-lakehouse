"""Orchestration du chargement bronze, mois par mois.

Pourquoi partitionner par mois plutôt que dérouler un seul curseur sur 15,3 M de
lignes : une boucle unique de ~1 530 requêtes est fragile (une coupure réseau à
80 % impose de tout refaire) et impossible à paralléliser. Découpé par mois, le
chargement devient reprenable — chaque mois est une unité de travail
indépendante et rejouable — et Airflow peut en traiter plusieurs de front.
"""

from __future__ import annotations

import calendar
import logging
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date

from .api import DpeApiClient, month_filter
from .columns import PARTITION_DATE_FIELD, select_clause
from .config import DATASET_START, IngestConfig
from .sink import BronzeSink, PartitionManifest

log = logging.getLogger(__name__)

TABLE = "dpe_existant"


@dataclass(frozen=True)
class Month:
    year: int
    month: int

    @property
    def first_day(self) -> date:
        return date(self.year, self.month, 1)

    @property
    def last_day(self) -> date:
        return date(self.year, self.month, calendar.monthrange(self.year, self.month)[1])

    def __str__(self) -> str:
        return f"{self.year:04d}-{self.month:02d}"


def iter_months(start: date, end: date) -> Iterator[Month]:
    """Énumère les mois couvrant [start, end], bornes incluses."""
    year, month = start.year, start.month
    while (year, month) <= (end.year, end.month):
        yield Month(year, month)
        year, month = (year + 1, 1) if month == 12 else (year, month + 1)


@dataclass
class BackfillReport:
    partitions_written: list[PartitionManifest]
    partitions_skipped: list[str]

    @property
    def total_rows(self) -> int:
        return sum(m.rows_written for m in self.partitions_written)

    @property
    def incomplete(self) -> list[PartitionManifest]:
        return [m for m in self.partitions_written if not m.is_complete]


def run_backfill(
    config: IngestConfig,
    start: date | None = None,
    end: date | None = None,
    force: bool = False,
    max_partitions: int | None = None,
) -> BackfillReport:
    """Charge en bronze tous les mois de la fenêtre demandée.

    Les partitions déjà complètes sont ignorées, sauf `force=True`. C'est ce qui
    rend la commande sûre à relancer : on ne retélécharge que ce qui manque.
    """
    client = DpeApiClient(config)
    sink = BronzeSink(config)
    sink.cleanup_staging(TABLE)

    start = start or DATASET_START
    end = end or date.today()
    select = select_clause()

    written: list[PartitionManifest] = []
    skipped: list[str] = []

    for month in iter_months(start, end):
        if max_partitions is not None and len(written) >= max_partitions:
            log.info("Limite de %d partitions atteinte — arrêt", max_partitions)
            break

        if not force and sink.is_partition_complete(TABLE, month.year, month.month):
            log.info("Partition %s déjà complète — ignorée", month)
            skipped.append(str(month))
            continue

        qs = month_filter(PARTITION_DATE_FIELD, month.first_day, month.last_day)
        announced = client.count_for_filter(qs)

        if announced == 0:
            log.info("Partition %s : aucune donnée en source — ignorée", month)
            skipped.append(str(month))
            continue

        log.info("Partition %s : %d lignes annoncées, téléchargement...", month, announced)

        manifest = sink.write_partition(
            table=TABLE,
            year=month.year,
            month=month.month,
            batches=client.iter_pages(qs=qs, select=select),
            rows_announced=announced,
            source_filter=qs,
        )
        written.append(manifest)

    report = BackfillReport(partitions_written=written, partitions_skipped=skipped)

    if report.incomplete:
        log.warning(
            "%d partition(s) incomplète(s) : %s",
            len(report.incomplete),
            ", ".join(m.partition for m in report.incomplete),
        )

    return report
