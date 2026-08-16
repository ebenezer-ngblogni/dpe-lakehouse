"""Tests de l'ingestion.

Aucun appel réseau : l'API est simulée avec `responses`, ce qui rend la suite
déterministe et exécutable en CI sans dépendre de la disponibilité de l'ADEME.
"""

from __future__ import annotations

from datetime import date

import pyarrow.parquet as pq
import pytest
import responses

from dpe_ingest.api import DpeApiClient, month_filter
from dpe_ingest.backfill import Month, iter_months
from dpe_ingest.columns import SELECTED_COLUMNS, select_clause
from dpe_ingest.config import IngestConfig
from dpe_ingest.sink import BronzeSink

LINES_URL = "https://data.ademe.fr/data-fair/api/v1/datasets/dpe03existant/lines"
DATASET_URL = "https://data.ademe.fr/data-fair/api/v1/datasets/dpe03existant"


@pytest.fixture
def config(tmp_path) -> IngestConfig:
    return IngestConfig(
        bronze_root=str(tmp_path / "bronze"),
        sleep_between_pages=0.0,
        max_retries=2,
    )


def csv_payload(rows: list[tuple[str, str]], bom: bool = True) -> bytes:
    """Génère une réponse CSV comme le fait l'API, BOM compris."""
    lignes = ['"numero_dpe","etiquette_dpe"']
    lignes += [f'"{numero}","{etiquette}"' for numero, etiquette in rows]
    body = ("\n".join(lignes) + "\n").encode("utf-8")
    return b"\xef\xbb\xbf" + body if bom else body


# ---------------------------------------------------------------------- #
# Découpage en mois
# ---------------------------------------------------------------------- #

def test_iter_months_couvre_les_bornes_incluses():
    mois = list(iter_months(date(2021, 11, 1), date(2022, 2, 15)))
    assert [str(m) for m in mois] == ["2021-11", "2021-12", "2022-01", "2022-02"]


def test_month_expose_premier_et_dernier_jour():
    fevrier_bissextile = Month(2024, 2)
    assert fevrier_bissextile.first_day == date(2024, 2, 1)
    assert fevrier_bissextile.last_day == date(2024, 2, 29)


def test_month_filter_produit_la_syntaxe_lucene():
    attendu = "date_etablissement_dpe:[2024-03-01 TO 2024-03-31]"
    assert month_filter("date_etablissement_dpe", date(2024, 3, 1), date(2024, 3, 31)) == attendu


# ---------------------------------------------------------------------- #
# Sélection de colonnes
# ---------------------------------------------------------------------- #

def test_les_colonnes_selectionnees_sont_uniques():
    assert len(SELECTED_COLUMNS) == len(set(SELECTED_COLUMNS))


def test_la_cle_primaire_et_le_watermark_sont_selectionnes():
    assert "numero_dpe" in SELECTED_COLUMNS
    assert "date_derniere_modification_dpe" in SELECTED_COLUMNS
    assert select_clause().count(",") == len(SELECTED_COLUMNS) - 1


# ---------------------------------------------------------------------- #
# Client API
# ---------------------------------------------------------------------- #

@responses.activate
def test_le_bom_utf8_ne_pollue_pas_le_nom_de_la_premiere_colonne(config):
    responses.add(responses.GET, LINES_URL, body=csv_payload([("A1", "D")]), status=200)

    pages = list(DpeApiClient(config).iter_pages(qs="peu importe", select="numero_dpe"))

    assert pages[0].column_names == ["numero_dpe", "etiquette_dpe"]


@responses.activate
def test_les_noms_de_colonnes_corrompus_sont_normalises(config):
    """L'export CSV de l'ADEME remplace un underscore par un espace dans
    certains noms : le schéma déclare `conso_5_usages_par_m2_ef`, le CSV émet
    `conso_5 usages_par_m2_ef`. Sans réparation, la colonne devient
    introuvable en aval et disparaît silencieusement du modèle."""
    corps = (
        b"\xef\xbb\xbf"
        b'"numero_dpe","conso_5 usages_par_m2_ef","emission_ges_5_usages par_m2"\n'
        b'"A1","73","5"\n'
    )
    responses.add(responses.GET, LINES_URL, body=corps, status=200)

    page = next(iter(DpeApiClient(config).iter_pages(qs="q", select="s")))

    assert page.column_names == [
        "numero_dpe",
        "conso_5_usages_par_m2_ef",
        "emission_ges_5_usages_par_m2",
    ]
    assert page.column("conso_5_usages_par_m2_ef").to_pylist() == ["73"]


@responses.activate
def test_la_pagination_suit_l_entete_link(config):
    page_suivante = f"{LINES_URL}?after=42"
    responses.add(
        responses.GET,
        LINES_URL,
        body=csv_payload([("A1", "D"), ("A2", "E")]),
        status=200,
        headers={"Link": f"<{page_suivante}>; rel=next"},
    )
    responses.add(responses.GET, page_suivante, body=csv_payload([("A3", "F")]), status=200)

    pages = list(DpeApiClient(config).iter_pages(qs="q", select="s"))

    assert [p.num_rows for p in pages] == [2, 1]


@responses.activate
def test_l_absence_d_entete_link_termine_la_pagination(config):
    responses.add(responses.GET, LINES_URL, body=csv_payload([("A1", "D")]), status=200)

    pages = list(DpeApiClient(config).iter_pages(qs="q", select="s"))

    assert len(pages) == 1


@responses.activate
def test_une_erreur_5xx_est_retentee_puis_reussit(config):
    responses.add(responses.GET, LINES_URL, status=503)
    responses.add(responses.GET, LINES_URL, body=csv_payload([("A1", "D")]), status=200)

    pages = list(DpeApiClient(config).iter_pages(qs="q", select="s"))

    assert pages[0].num_rows == 1
    assert len(responses.calls) == 2


@responses.activate
def test_count_for_filter_lit_le_total_annonce(config):
    responses.add(responses.GET, LINES_URL, json={"total": 247783, "results": []}, status=200)

    assert DpeApiClient(config).count_for_filter("q") == 247783


# ---------------------------------------------------------------------- #
# Écriture bronze
# ---------------------------------------------------------------------- #

def _table(rows: list[tuple[str, str]]):
    from dpe_ingest.api import DpeApiClient as Client

    return Client._parse_csv(csv_payload(rows))


def test_la_partition_est_ecrite_avec_son_manifeste(config):
    sink = BronzeSink(config)

    manifeste = sink.write_partition(
        table="dpe_existant",
        year=2024,
        month=3,
        batches=[_table([("A1", "D"), ("A2", "E")])],
        rows_announced=2,
        source_filter="q",
    )

    assert manifeste.rows_written == 2
    assert manifeste.is_complete
    assert sink.is_partition_complete("dpe_existant", 2024, 3)


def test_un_ecart_avec_la_source_marque_la_partition_incomplete(config):
    sink = BronzeSink(config)

    manifeste = sink.write_partition(
        table="dpe_existant",
        year=2024,
        month=3,
        batches=[_table([("A1", "D")])],
        rows_announced=5,  # la source en annonçait 5, on n'en a écrit qu'une
        source_filter="q",
    )

    assert not manifeste.is_complete
    assert not sink.is_partition_complete("dpe_existant", 2024, 3)


def test_reecrire_une_partition_ne_duplique_pas_les_lignes(config):
    """L'idempotence est la garantie centrale du pipeline : deux écritures
    successives doivent laisser exactement le même contenu."""
    sink = BronzeSink(config)

    for _ in range(2):
        sink.write_partition(
            table="dpe_existant",
            year=2024,
            month=3,
            batches=[_table([("A1", "D"), ("A2", "E")])],
            rows_announced=2,
            source_filter="q",
        )

    chemin = f"{config.bronze_root}/dpe_existant/annee=2024/mois=03"
    table = pq.read_table(chemin)

    assert table.num_rows == 2


def test_la_partition_temporaire_disparait_apres_promotion(config):
    """Après promotion, plus aucune donnée ne doit subsister côté temporaire.

    Seul le répertoire `_staging` parent peut rester, vide : sur un stockage
    objet il n'existe de toute façon pas en tant qu'entité, et `cleanup_staging`
    le purge au run suivant.
    """
    sink = BronzeSink(config)
    sink.write_partition(
        table="dpe_existant",
        year=2024,
        month=3,
        batches=[_table([("A1", "D")])],
        rows_announced=1,
        source_filter="q",
    )

    staging = f"{config.bronze_root}/dpe_existant/_staging"
    assert not sink.fs.exists(f"{staging}/annee=2024_mois=03")
    assert sink.fs.find(staging) == []
