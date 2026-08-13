"""Interface en ligne de commande de l'ingestion bronze.

Exemples :
    python -m dpe_ingest backfill --start 2026-01 --end 2026-03
    python -m dpe_ingest backfill --max-partitions 1        # bac à sable
    python -m dpe_ingest status
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date

from .api import DpeApiClient
from .backfill import TABLE, iter_months, run_backfill
from .config import DATASET_START, IngestConfig
from .sink import BronzeSink


def _parse_month(value: str) -> date:
    """Accepte AAAA-MM ou AAAA-MM-JJ."""
    parts = value.split("-")
    try:
        if len(parts) == 2:
            return date(int(parts[0]), int(parts[1]), 1)
        if len(parts) == 3:
            return date(int(parts[0]), int(parts[1]), int(parts[2]))
    except ValueError:
        pass
    raise argparse.ArgumentTypeError(f"Date invalide : {value!r} (attendu AAAA-MM)")


def cmd_backfill(args: argparse.Namespace) -> int:
    config = IngestConfig()
    report = run_backfill(
        config=config,
        start=args.start,
        end=args.end,
        force=args.force,
        max_partitions=args.max_partitions,
    )

    print()
    print(f"  Partitions écrites  : {len(report.partitions_written)}")
    print(f"  Partitions ignorées : {len(report.partitions_skipped)}")
    print(f"  Lignes chargées     : {report.total_rows:,}".replace(",", " "))

    if report.incomplete:
        print(f"  ATTENTION — {len(report.incomplete)} partition(s) incomplète(s)")
        return 1
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    """Compare l'état local à la source, partition par partition."""
    config = IngestConfig()
    sink = BronzeSink(config)
    client = DpeApiClient(config)

    print(f"Source  : {config.dataset_url}")
    print(f"Bronze  : {config.bronze_root}")
    print(f"Total en source : {client.total_records():,}".replace(",", " "))
    print()
    print(f"{'PARTITION':<16}{'LIGNES':>12}{'SOURCE':>12}   ÉTAT")

    total_local = 0
    missing = 0
    for month in iter_months(DATASET_START, date.today()):
        manifest = sink.read_manifest(TABLE, month.year, month.month)
        if manifest is None:
            missing += 1
            if args.verbose:
                print(f"{str(month):<16}{'-':>12}{'-':>12}   absente")
            continue
        total_local += manifest.rows_written
        state = "ok" if manifest.is_complete else "INCOMPLÈTE"
        print(
            f"{str(month):<16}{manifest.rows_written:>12,}"
            f"{manifest.rows_announced_by_source:>12,}   {state}".replace(",", " ")
        )

    print()
    print(f"Total local : {total_local:,}".replace(",", " "))
    print(f"Partitions absentes : {missing}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="dpe_ingest",
        description="Ingestion de la base DPE de l'ADEME vers la couche bronze.",
    )
    parser.add_argument(
        "--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"]
    )
    sub = parser.add_subparsers(dest="command", required=True)

    backfill = sub.add_parser("backfill", help="Charge une fenêtre de mois en bronze")
    backfill.add_argument("--start", type=_parse_month, default=None, help="Mois de début (AAAA-MM)")
    backfill.add_argument("--end", type=_parse_month, default=None, help="Mois de fin (AAAA-MM)")
    backfill.add_argument(
        "--force", action="store_true", help="Recharge même les partitions déjà complètes"
    )
    backfill.add_argument(
        "--max-partitions", type=int, default=None, help="Plafonne le nombre de mois traités"
    )
    backfill.set_defaults(func=cmd_backfill)

    status = sub.add_parser("status", help="Compare l'état de bronze à la source")
    status.add_argument("--verbose", action="store_true", help="Affiche aussi les mois absents")
    status.set_defaults(func=cmd_status)

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s  %(levelname)-7s %(name)s  %(message)s",
        datefmt="%H:%M:%S",
    )
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
