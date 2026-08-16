"""Client de l'API Data Fair de l'ADEME.

Deux contraintes dictent la conception, toutes deux mesurées sur l'API réelle
plutôt que supposées :

1. `size` est plafonné à 10 000 lignes et la pagination se fait par curseur
   opaque (`after`), pas par offset.
2. Le format de sortie pèse lourd. Sur une page identique de 10 000 lignes et
   66 colonnes, non mise en cache côté serveur :

       JSON + gzip : 36,9 s   (3,4 Mo)
       CSV  + gzip : 13,0 s   (2,0 Mo)

   Le JSON répète les 66 noms de colonnes à chaque ligne. Passer en CSV divise
   le temps de chargement complet par 2,8 — de ~34 h à ~5,5 h.

En CSV, le lien de page suivante n'est plus dans le corps mais dans l'en-tête
HTTP `Link: <...>; rel=next`, que `requests` expose via `response.links`.
"""

from __future__ import annotations

import logging
import re
import time
from collections.abc import Iterator
from datetime import date
from typing import Any

import pyarrow as pa
import requests
from pyarrow import csv as pa_csv

from .config import IngestConfig

log = logging.getLogger(__name__)


class ApiError(RuntimeError):
    """Échec d'appel à l'API après épuisement des tentatives."""


class DpeApiClient:
    def __init__(self, config: IngestConfig, session: requests.Session | None = None) -> None:
        self.config = config
        self.session = session or requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "dpe-lakehouse/1.0 (projet portfolio data)",
                # requests décompresse de façon transparente ; on l'annonce
                # explicitement car le gain est décisif ici.
                "Accept-Encoding": "gzip, deflate",
            }
        )

    # ------------------------------------------------------------------ #
    # Appel unitaire avec réessais
    # ------------------------------------------------------------------ #
    def _request(self, url: str, params: dict[str, Any] | None = None) -> requests.Response:
        last_error: Exception | None = None

        for attempt in range(1, self.config.max_retries + 1):
            try:
                response = self.session.get(
                    url, params=params, timeout=self.config.timeout_seconds
                )
                # 429 (quota) et 5xx sont transitoires : on réessaie.
                if response.status_code == 429 or response.status_code >= 500:
                    raise requests.HTTPError(f"HTTP {response.status_code}", response=response)
                response.raise_for_status()
                return response

            except (requests.RequestException, ValueError) as exc:
                last_error = exc
                if attempt == self.config.max_retries:
                    break
                delay = self.config.backoff_base**attempt
                log.warning(
                    "Appel API échoué (tentative %d/%d) : %s — nouvelle tentative dans %.1fs",
                    attempt,
                    self.config.max_retries,
                    exc,
                    delay,
                )
                time.sleep(delay)

        raise ApiError(f"Abandon après {self.config.max_retries} tentatives : {last_error}")

    # ------------------------------------------------------------------ #
    # Métadonnées
    # ------------------------------------------------------------------ #
    def total_records(self) -> int:
        """Nombre total de lignes du jeu de données côté source."""
        return int(self._request(self.config.dataset_url).json().get("count", 0))

    def count_for_filter(self, qs: str) -> int:
        """Nombre de lignes correspondant à un filtre, sans les rapatrier.

        Sert de contrôle de complétude : on compare ce que la source annonce à ce
        qu'on a réellement écrit sur disque.
        """
        payload = self._request(self.config.lines_url, params={"size": 0, "qs": qs}).json()
        return int(payload.get("total", 0))

    # ------------------------------------------------------------------ #
    # Pagination CSV
    # ------------------------------------------------------------------ #
    def iter_pages(self, qs: str, select: str) -> Iterator[pa.Table]:
        """Itère les pages d'un filtre, converties en tables Arrow.

        Chaque colonne est forcée en texte : la couche bronze conserve la donnée
        telle que la source l'expose, le typage relève de silver. Une valeur
        aberrante ne peut donc pas faire échouer l'ingestion.
        """
        params: dict[str, Any] | None = {
            "size": self.config.page_size,
            "qs": qs,
            "select": select,
            "format": "csv",
        }
        url: str | None = self.config.lines_url
        pages = 0

        while url:
            response = self._request(url, params=params)
            table = self._parse_csv(response.content)

            if table.num_rows == 0:
                break

            pages += 1
            yield table

            # Le lien `next` embarque déjà tous les paramètres de la requête.
            next_link = response.links.get("next")
            url = next_link.get("url") if next_link else None
            params = None

            if url and self.config.sleep_between_pages:
                time.sleep(self.config.sleep_between_pages)

        log.debug("Filtre %s : %d page(s) parcourue(s)", qs, pages)

    @staticmethod
    def _parse_csv(payload: bytes) -> pa.Table:
        """Convertit une réponse CSV en table Arrow entièrement typée en texte."""
        # L'API préfixe sa sortie d'un BOM UTF-8, qui polluerait le nom de la
        # première colonne s'il n'était pas retiré.
        if payload.startswith(b"\xef\xbb\xbf"):
            payload = payload[3:]

        if not payload.strip():
            return pa.table({})

        header_line = payload.split(b"\n", 1)[0].decode("utf-8")
        # L'export CSV de l'API corrompt certains noms de colonnes en
        # remplaçant un underscore par un espace : le schéma JSON déclare
        # `conso_5_usages_par_m2_ef`, le CSV émet `conso_5 usages_par_m2_ef`.
        # Aucune clé du schéma ADEME ne contient d'espace, la normalisation est
        # donc sans risque de collision.
        columns = [
            re.sub(r"\s+", "_", name.strip().strip('"')) for name in header_line.split(",")
        ]

        table = pa_csv.read_csv(
            pa.BufferReader(payload),
            convert_options=pa_csv.ConvertOptions(strings_can_be_null=True),
            read_options=pa_csv.ReadOptions(column_names=columns, skip_rows=1),
        )
        # Tout en texte : bronze conserve la donnée telle que la source
        # l'expose, le typage relève de silver.
        return table.cast(pa.schema([(nom, pa.string()) for nom in table.column_names]))


def month_filter(field: str, start: date, end: date) -> str:
    """Construit un filtre Lucene de plage de dates inclusive.

    Exemple : date_etablissement_dpe:[2026-01-01 TO 2026-01-31]
    """
    return f"{field}:[{start.isoformat()} TO {end.isoformat()}]"
