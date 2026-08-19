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
from pyproj import Transformer

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
    # Ajoute latitude/longitude WGS84 depuis les centroïdes Lambert 93.
    reprojeter: bool = False

    @property
    def qualifie(self) -> str:
        return f"{self.schema}.{self.nom}"


TABLES = [
    Table("marts", "dim_commune", "csv", reprojeter=True),
    Table("marts", "mart_performance_commune", "csv", reprojeter=True),
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


# Power BI cartographie en WGS84 (latitude/longitude) ; l'ADEME publie en
# Lambert 93 (EPSG:2154), la projection officielle de la France métropolitaine.
#
# La conversion est faite ici plutôt que dans dbt ou dans le rapport :
#   - dans l'entrepôt, elle exigerait PostGIS ou une réimplémentation en SQL
#     d'une projection conique conforme, avec résolution itérative de la latitude ;
#   - dans Power Query, elle serait à refaire dans chaque rapport et invisible
#     pour quiconque relit le pipeline.
# L'export est la frontière vers la couche de restitution : reprojeter pour
# l'outil qui consomme y est légitime.
LAMBERT93_VERS_WGS84 = Transformer.from_crs("EPSG:2154", "EPSG:4326", always_xy=True)

# Emprise de validité de Lambert 93 : la France métropolitaine, Corse comprise.
#
# Les DOM n'en relèvent pas — les Antilles sont en UTM 20N, la Guyane en UTM 22N,
# La Réunion en UTM 40S. Leurs coordonnées « Lambert 93 » dans la source sont
# donc dénuées de sens et se reprojettent n'importe où. Mesuré ici : 46 lignes
# sur les départements 971 à 974, plus une trentaine de centroïdes métropolitains
# calculés sur des coordonnées source aberrantes.
#
# 76 lignes sur 69 134, mais elles suffisent à semer des points en pleine mer.
# On annule leur géolocalisation plutôt que de les supprimer : la donnée
# énergétique de ces communes reste valable, seule leur position est perdue.
LATITUDE_MIN, LATITUDE_MAX = 41.0, 51.5
LONGITUDE_MIN, LONGITUDE_MAX = -5.5, 10.0


def ajouter_coordonnees_wgs84(table: pa.Table) -> pa.Table:
    """Ajoute latitude/longitude à partir des centroïdes en Lambert 93.

    Les communes sans centroïde (aucun DPE au géocodage précis) restent nulles :
    Power BI les ignore simplement sur la carte.
    """
    if "centroide_x_lambert93" not in table.column_names:
        return table

    xs = table.column("centroide_x_lambert93").to_pylist()
    ys = table.column("centroide_y_lambert93").to_pylist()

    latitudes: list[float | None] = []
    longitudes: list[float | None] = []
    for x, y in zip(xs, ys):
        if x is None or y is None or x == "" or y == "":
            latitudes.append(None)
            longitudes.append(None)
            continue
        lon, lat = LAMBERT93_VERS_WGS84.transform(float(x), float(y))
        if not (LATITUDE_MIN <= lat <= LATITUDE_MAX and LONGITUDE_MIN <= lon <= LONGITUDE_MAX):
            latitudes.append(None)
            longitudes.append(None)
            continue
        latitudes.append(round(lat, 6))
        longitudes.append(round(lon, 6))

    return table.append_column("latitude", pa.array(latitudes, type=pa.float64())) \
                .append_column("longitude", pa.array(longitudes, type=pa.float64()))


def reprojeter_fichier_csv(chemin: Path) -> int:
    """Relit un CSV exporté, y ajoute latitude/longitude, et le réécrit.

    Renvoie le nombre de lignes effectivement géolocalisées.
    """
    table = pa_csv.read_csv(
        chemin,
        convert_options=pa_csv.ConvertOptions(strings_can_be_null=True),
    )
    enrichie = ajouter_coordonnees_wgs84(table)
    if "latitude" not in enrichie.column_names:
        return 0
    pa_csv.write_csv(enrichie, chemin)
    return sum(1 for v in enrichie.column("latitude").to_pylist() if v is not None)


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
    parseur.add_argument(
        "--table",
        action="append",
        default=None,
        help="N'exporte que ces tables (répétable). Défaut : toutes.",
    )
    args = parseur.parse_args()

    tables = TABLES if not args.table else [t for t in TABLES if t.nom in args.table]
    if not tables:
        print(f"Aucune table ne correspond à {args.table}", file=sys.stderr)
        return 1

    destination: Path = args.destination
    destination.mkdir(parents=True, exist_ok=True)

    print(f"Destination : {destination.resolve()}\n")

    for table in tables:
        attendu = compter(table)
        fichier = destination / f"{table.nom}.{table.format}"
        print(f"  {table.qualifie:34s} {attendu:>10,} lignes → {fichier.name}".replace(",", " "))

        if table.format == "csv":
            octets = exporter_csv(table, fichier)
            if table.reprojeter:
                ajoutees = reprojeter_fichier_csv(fichier)
                octets = fichier.stat().st_size
                print(f"  {'':34s} {ajoutees:>10,} coordonnées WGS84 ajoutées".replace(",", " "))
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
