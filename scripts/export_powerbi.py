#!/usr/bin/env python3
"""Exporte les tables `marts` vers des fichiers lisibles par Power BI Desktop.

Pourquoi un export plutôt qu'une connexion directe : Power BI Desktop n'existe
que sous Windows. Sur un poste en double amorçage, redémarrer sous Windows
éteint la pile Docker, donc l'entrepôt PostgreSQL devient injoignable. Écrire
les tables sur une partition que les deux systèmes lisent résout le problème
sans rien perdre — Power BI travaille de toute façon en mode Import.

Formats retenus :
  - CSV pour les dimensions et l'agrégat : universels, ouvrables partout
  - Parquet pour la table de faits : 7,8 M de lignes en CSV pèseraient ~2,5 Go
    et mettraient plusieurs minutes à s'importer, contre ~400 Mo en Parquet

L'export passe par `psql COPY ... TO STDOUT`, dont la sortie est traitée en
flux : aucune table n'est jamais chargée entièrement en mémoire.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import csv as pa_csv

CONTENEUR = "dpe_warehouse"
BASE = "dpe"
UTILISATEUR = "dpe"

# Taille des lots lus depuis psql. 64 Mo est un compromis : assez grand pour
# limiter les allers-retours, assez petit pour que l'empreinte mémoire reste
# modeste sur un poste chargé.
TAILLE_BLOC = 64 * 1024 * 1024


@dataclass(frozen=True)
class Table:
    schema: str
    nom: str
    format: str  # "csv" ou "parquet"

    @property
    def qualifie(self) -> str:
        return f"{self.schema}.{self.nom}"


TABLES = [
    Table("marts", "dim_commune", "csv"),
    Table("marts", "mart_performance_commune", "csv"),
    Table("marts", "fct_dpe", "parquet"),
]


def _psql(requete: str) -> subprocess.Popen:
    """Lance une requête dans le conteneur et renvoie le processus, sortie ouverte."""
    return subprocess.Popen(
        ["docker", "exec", "-i", CONTENEUR, "psql", "-U", UTILISATEUR, "-d", BASE,
         "-A", "-q", "-c", requete],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def compter(table: Table) -> int:
    resultat = subprocess.run(
        ["docker", "exec", CONTENEUR, "psql", "-U", UTILISATEUR, "-d", BASE,
         "-tAc", f"select count(*) from {table.qualifie}"],
        capture_output=True, text=True, check=True,
    )
    return int(resultat.stdout.strip())


def exporter_csv(table: Table, destination: Path) -> int:
    """Écrit la table telle quelle en CSV, en-tête compris."""
    requete = f"copy (select * from {table.qualifie}) to stdout with (format csv, header true)"
    with destination.open("wb") as sortie:
        processus = _psql(requete)
        octets = 0
        assert processus.stdout is not None
        for bloc in iter(lambda: processus.stdout.read(TAILLE_BLOC), b""):
            sortie.write(bloc)
            octets += len(bloc)
        processus.wait()
        if processus.returncode != 0:
            erreur = (processus.stderr.read() if processus.stderr else b"").decode()
            raise RuntimeError(f"Export de {table.qualifie} en échec : {erreur}")
    return octets


# Correspondance entre types PostgreSQL et types Arrow.
# On ne laisse pas pyarrow deviner : il infère le type sur le premier lot lu,
# et se trompe dès qu'une valeur atypique apparaît plus loin. Cas réel
# rencontré ici : `code_commune` semblait numérique jusqu'à la commune corse
# « 2A006 » — les codes INSEE de Corse-du-Sud (2A) et Haute-Corse (2B) ne sont
# pas des nombres. L'export échouait alors après plusieurs minutes.
TYPES_POSTGRES = {
    "text": pa.string(), "character varying": pa.string(), "character": pa.string(),
    "smallint": pa.int16(), "integer": pa.int32(), "bigint": pa.int64(),
    "numeric": pa.float64(), "real": pa.float64(), "double precision": pa.float64(),
    "boolean": pa.bool_(),
    "date": pa.date32(),
    "timestamp without time zone": pa.timestamp("us"),
    "timestamp with time zone": pa.timestamp("us", tz="UTC"),
}


def schema_depuis_postgres(table: Table) -> pa.Schema:
    """Construit le schéma Arrow à partir des types déclarés par PostgreSQL."""
    resultat = subprocess.run(
        ["docker", "exec", CONTENEUR, "psql", "-U", UTILISATEUR, "-d", BASE, "-tAF", "|", "-c",
         f"""select column_name, data_type from information_schema.columns
             where table_schema = '{table.schema}' and table_name = '{table.nom}'
             order by ordinal_position"""],
        capture_output=True, text=True, check=True,
    )

    champs = []
    for ligne in resultat.stdout.strip().splitlines():
        if not ligne.strip():
            continue
        nom, type_pg = ligne.split("|", 1)
        # Un type inconnu retombe sur du texte : mieux vaut une colonne
        # exploitable en chaîne qu'un export qui échoue.
        champs.append(pa.field(nom, TYPES_POSTGRES.get(type_pg.strip(), pa.string())))
    return pa.schema(champs)


def exporter_parquet(table: Table, destination: Path) -> int:
    """Convertit le flux CSV de psql en Parquet, par lots.

    Le schéma vient de PostgreSQL, pas de l'inférence : voir TYPES_POSTGRES.
    """
    schema = schema_depuis_postgres(table)
    requete = f"copy (select * from {table.qualifie}) to stdout with (format csv, header true)"
    processus = _psql(requete)
    assert processus.stdout is not None

    lecteur = pa_csv.open_csv(
        processus.stdout,
        read_options=pa_csv.ReadOptions(block_size=TAILLE_BLOC),
        convert_options=pa_csv.ConvertOptions(
            column_types={champ.name: champ.type for champ in schema},
            strings_can_be_null=True,
            # PostgreSQL exporte les booléens en « t » / « f », que pyarrow ne
            # reconnaît pas : il attend « true » / « false ».
            true_values=["t", "true", "TRUE", "True"],
            false_values=["f", "false", "FALSE", "False"],
        ),
    )

    ecrivain: pq.ParquetWriter | None = None
    lignes = 0
    try:
        for lot in lecteur:
            table_arrow = pa.Table.from_batches([lot])
            if ecrivain is None:
                ecrivain = pq.ParquetWriter(destination, table_arrow.schema, compression="snappy")
            ecrivain.write_table(table_arrow)
            lignes += table_arrow.num_rows
    finally:
        if ecrivain is not None:
            ecrivain.close()
        processus.wait()

    if processus.returncode != 0:
        erreur = (processus.stderr.read() if processus.stderr else b"").decode()
        raise RuntimeError(f"Export de {table.qualifie} en échec : {erreur}")
    return lignes


def formater(octets: int) -> str:
    for unite in ("o", "Ko", "Mo", "Go"):
        if octets < 1024 or unite == "Go":
            return f"{octets:.0f} {unite}" if unite == "o" else f"{octets:.1f} {unite}"
        octets /= 1024.0
    return f"{octets:.1f} Go"


def main() -> int:
    parseur = argparse.ArgumentParser(description=__doc__)
    parseur.add_argument(
        "--destination",
        type=Path,
        default=Path("data/exports/powerbi"),
        help="Répertoire de sortie (défaut : data/exports/powerbi, sur le disque du lac)",
    )
    args = parseur.parse_args()

    destination: Path = args.destination
    destination.mkdir(parents=True, exist_ok=True)

    print(f"Destination : {destination.resolve()}\n")

    for table in TABLES:
        attendu = compter(table)
        fichier = destination / f"{table.nom}.{table.format}"
        print(f"  {table.qualifie:34s} {attendu:>10,} lignes → {fichier.name}".replace(",", " "))

        if table.format == "csv":
            octets = exporter_csv(table, fichier)
            print(f"  {'':34s} {formater(octets):>10s} écrits\n")
        else:
            lignes = exporter_parquet(table, fichier)
            if lignes != attendu:
                print(f"  ÉCART : {lignes} lignes écrites pour {attendu} attendues", file=sys.stderr)
                return 1
            print(f"  {'':34s} {formater(fichier.stat().st_size):>10s} écrits, "
                  f"{lignes:,} lignes vérifiées\n".replace(",", " "))

    print("Export terminé. Sous Windows, ouvrir ces fichiers depuis Power BI Desktop.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
