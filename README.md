# Lakehouse DPE — de l'API ADEME à Power BI

Pipeline de données complet sur les **diagnostics de performance énergétique des
logements français** : 15,3 millions d'enregistrements réels, ingérés depuis
l'API publique de l'ADEME, nettoyés par un job **Spark en Scala**, modélisés en
étoile avec **dbt**, orchestrés par **Airflow** et restitués dans **Power BI**.

Le sujet n'est pas un prétexte : depuis la loi Climat et Résilience, les
logements classés F et G — les « passoires thermiques » — sortent
progressivement du marché locatif. Savoir où ils se trouvent, dans quel état est
le parc et comment il évolue est une question qui se pose réellement aux
collectivités, aux bailleurs et aux acteurs de la rénovation.

```
   API ADEME              MinIO (S3)                  PostgreSQL           Power BI
  15,3 M de DPE   ──▶   bronze ──▶ silver   ──▶   silver ──▶ marts   ──▶   rapport
                          Parquet partitionné        modèle en étoile
     Python              Spark 3.5 / Scala 2.12            dbt
                    ╰──────────── Airflow ────────────╯
```

---

## Sommaire

- [Ce que le projet démontre](#ce-que-le-projet-démontre)
- [Architecture](#architecture)
- [Les problèmes réels rencontrés](#les-problèmes-réels-rencontrés)
- [Démarrage rapide](#démarrage-rapide)
- [Structure du dépôt](#structure-du-dépôt)
- [Qualité des données](#qualité-des-données)
- [Tests](#tests)
- [Limites connues](#limites-connues)

---

## Ce que le projet démontre

| Compétence | Où la voir |
|---|---|
| Ingestion d'API paginée, incrémentale et reprenable | `ingestion/dpe_ingest/` |
| Écriture idempotente avec promotion atomique | `ingestion/dpe_ingest/sink.py` |
| Spark en Scala : typage, déduplication, résolution d'identifiants | `spark-jobs/src/main/scala/fr/dpelab/silver/` |
| Tests unitaires Spark sans cluster | `spark-jobs/src/test/scala/` |
| Modélisation dimensionnelle et tests de données | `dbt/dpe_analytics/models/` |
| Orchestration et fenêtre glissante de rattrapage | `airflow/dags/dpe_pipeline.py` |
| Infrastructure reproductible | `docker-compose.yml`, `Makefile` |
| Intégration continue | `.github/workflows/ci.yml` |

---

## Architecture

### Bronze — fidélité à la source

Toutes les colonnes sont stockées **en texte**, exactement telles que l'API les
expose. Aucun typage, aucun filtre, aucune correction.

Ce choix est délibéré : une valeur aberrante ne doit jamais faire échouer une
ingestion. Si l'ADEME publie demain `surface_habitable_logement = "non
renseigné"`, le chargement continue et l'anomalie est traitée en aval, là où
elle est visible et documentée. Un typage à l'ingestion aurait transformé cette
donnée sale en incident de production.

Partitionnement : `annee=AAAA/mois=MM` sur la date d'établissement du DPE.
Chaque partition porte un `_manifest.json` qui enregistre le nombre de lignes
écrites **et** le nombre annoncé par la source — l'écart entre les deux est le
premier signal d'un chargement incomplet.

### Silver — la donnée exploitable

Le job Scala applique dans l'ordre : typage explicite, déduplication sur
`numero_dpe`, résolution des chaînes de remplacement, règles de qualité,
enrichissements analytiques.

Il produit **trois** sorties, pas une :

- `dpe_courant` — un DPE valide et à jour par logement
- `dpe_rejets` — les lignes écartées **avec leur motif**
- `_metrics` — les compteurs du run

Conserver les rejets est un principe : un pipeline qui jette des données
silencieusement ne peut pas être audité. Quand un analyste demande « pourquoi ce
logement n'apparaît-il pas ? », la réponse est dans une table, pas dans un log
effacé depuis trois semaines.

### Gold — le modèle en étoile

dbt construit `fct_dpe` (grain : un diagnostic), `dim_commune`, et l'agrégat
`mart_performance_commune` qui alimente directement la carte Power BI.

**Frontière de responsabilité :** Spark fait le gros œuvre sur le lac
(15 M de lignes, déduplication, jointures larges), l'entrepôt ne reçoit que le
résultat propre, dbt y fait la modélisation en SQL. Dédoublonner dans PostgreSQL
serait possible mais lent ; modéliser en étoile dans Spark priverait le projet
du lignage, des tests et de la documentation que dbt fournit sans effort.

---

## Les problèmes réels rencontrés

Cette section est volontairement détaillée : c'est là que se trouve l'essentiel
du travail d'ingénierie, et ce dont il est le plus intéressant de discuter.

### 1. Le format de transport coûtait 2,8× trop cher

L'API renvoie du JSON par défaut. Sur une page de 10 000 lignes et 66 colonnes,
mesurée hors cache serveur :

| Format | Temps | Volume reçu |
|---|---|---|
| JSON + gzip | 36,9 s | 3,4 Mo |
| **CSV + gzip** | **13,0 s** | **2,0 Mo** |

Le JSON répète les 66 noms de colonnes à chaque ligne. Passer en CSV ramène le
chargement complet de **~34 h à ~5,5 h**.

Conséquence technique : en CSV, le curseur de pagination n'est plus dans le
corps de la réponse mais dans l'en-tête HTTP `Link: <...>; rel=next`.

### 2. Un curseur unique sur 15,3 M de lignes est ingérable

Une boucle de ~1 530 requêtes séquentielles est fragile : une coupure à 80 %
impose de tout reprendre, et rien n'est parallélisable.

Le chargement est donc découpé **par mois**, via un filtre de plage de dates.
Chaque mois devient une unité de travail indépendante, rejouable, et vérifiable
par comparaison au total annoncé par la source. C'est ce découpage qui rend le
`make ingest-full` interruptible : relancé, il reprend là où il s'était arrêté.

### 3. Les DPE se remplacent les uns les autres

8,9 % des diagnostics référencent un DPE antérieur via `numero_dpe_remplace`.
Sans traitement, un même logement apparaît plusieurs fois avec des étiquettes
différentes — et le taux de passoires thermiques devient faux.

La règle : un DPE est périmé dès lors qu'il figure dans la colonne
`numero_dpe_remplace` d'un autre enregistrement. La source chaînant
`A ← B ← C` en deux liens distincts, une résolution à un seul niveau suffit à
marquer A et B comme obsolètes — inutile de calculer une fermeture transitive.
Ce cas est couvert par un test dédié.

### 4. Le géocodage n'est pas fiable partout

`score_ban` mesure la confiance du rapprochement d'adresse. Des valeurs à 0,48
ou 0,62 apparaissent dès les premières lignes.

Choix retenu : **ne pas rejeter** ces lignes — la donnée énergétique reste
valable — mais les marquer via `geocodage_fiable`. Les centroïdes communaux du
modèle sont calculés sur les seuls points fiables, sinon les adresses mal
rapprochées tireraient le point vers le centre du département.

### 5. Spark sur JDK 17

`IllegalAccessError: class SparkDateTimeUtils cannot access class
sun.util.calendar.ZoneInfo`. Le JDK 16 a encapsulé les paquets internes ; Spark
y accède toujours par réflexion dès qu'une colonne de type date est manipulée.
Résolu par les `--add-opens` dans `build.sbt`.

---

## Démarrage rapide

### Prérequis

- Docker et Docker Compose
- Java 17 et sbt (`cs install sbt` via [Coursier](https://get-coursier.io/))
- Python 3.11+
- Power BI Desktop (Windows) pour la restitution

### Espace disque

| Emplacement | Besoin | Contrainte |
|---|---|---|
| Lac de données (`data/`) | ~6 Go | N'importe quel système de fichiers, y compris NTFS |
| Volumes Docker et entrepôt | ~10 Go | **Système de fichiers Linux obligatoire** |

Le lac ne contient que des fichiers Parquet : il peut vivre sur un disque
externe, y compris en NTFS. Placer `data/` ailleurs se fait par lien symbolique
ou via la variable `DPE_BRONZE_ROOT`.

Les volumes Docker, eux, ne peuvent pas y aller : PostgreSQL et MinIO tournent
sous des UID dédiés et doivent posséder leur répertoire de données, or NTFS
refuse `chown`. `initdb` échouerait au démarrage.

### Installation

```bash
git clone <url-du-dépôt> && cd DataEng
cp .env.example .env          # puis adapter les mots de passe
make setup                    # environnement Python
make build                    # compile le jar Spark
```

### Un premier essai en deux minutes

```bash
make ingest-sample            # charge un seul mois (~80 000 DPE)
make silver                   # transformation Spark en local
```

### La chaîne complète

```bash
make up                       # MinIO + PostgreSQL + Spark + Airflow
make ingest-full              # ~5 h, interruptible et reprenable
make dbt-run && make dbt-test
```

| Service | URL | Identifiants |
|---|---|---|
| Airflow | http://localhost:8080 | `admin` / `admin` |
| MinIO | http://localhost:9001 | voir `.env` |
| Entrepôt | `postgresql://localhost:5433/dpe` | voir `.env` |

### Suivre l'avancement

```bash
make ingest-status            # compare chaque partition locale à la source
```

---

## Structure du dépôt

```
├── ingestion/            Ingestion Python : API → bronze (Parquet)
│   ├── dpe_ingest/
│   │   ├── api.py        Client Data Fair, pagination CSV, réessais
│   │   ├── sink.py       Écriture Parquet atomique + manifestes
│   │   ├── backfill.py   Découpage mensuel et reprise
│   │   └── columns.py    63 colonnes retenues sur 230
│   └── tests/
├── spark-jobs/           Jobs Spark en Scala
│   └── src/main/scala/fr/dpelab/
│       ├── silver/       Transformations bronze → silver
│       └── warehouse/    Chargement silver → PostgreSQL
├── dbt/dpe_analytics/    Modélisation en étoile et tests de données
├── airflow/dags/         Orchestration hebdomadaire
├── docker/               Images Spark et Airflow
├── powerbi/              Modèle et captures du rapport
└── scripts/              Initialisation de l'entrepôt
```

---

## Qualité des données

Huit règles écartent les lignes inexploitables, chacune avec un motif traçable :

| Motif | Règle |
|---|---|
| `identifiant_absent` | `numero_dpe` vide |
| `etiquette_dpe_invalide` | Étiquette hors A–G |
| `date_etablissement_absente` | Date illisible ou absente |
| `commune_absente` | Code INSEE manquant |
| `surface_invalide` | Surface nulle ou négative |
| `surface_aberrante` | Surface > 1 000 m² pour un logement |
| `consommation_invalide` | Consommation négative |
| `consommation_aberrante` | > 2 000 kWh/m²/an — erreur d'unité ou de saisie |

Les seuils sont volontairement larges : l'objectif est d'écarter les erreurs de
saisie manifestes, pas de lisser la réalité du parc immobilier.

Côté dbt, les sources portent des tests `unique`, `not_null`,
`accepted_values` et un contrôle de fraîcheur — la base ADEME étant
hebdomadaire, deux semaines sans nouveau DPE signalent une panne de chargement.

---

## Tests

```bash
make test        # Scala + Python
make lint
```

**Scala (11 tests)** — typage tolérant aux valeurs illisibles, déduplication,
chaînes de remplacement à trois maillons, règles de qualité, idempotence de la
chaîne complète.

**Python (14 tests)** — découpage mensuel, pagination par en-tête `Link`, BOM
UTF-8, réessai sur erreur 5xx, écriture atomique, détection d'écart avec la
source, non-duplication en cas de réécriture.

Les tests Python n'appellent jamais le réseau : l'API est simulée, la suite est
déterministe et tourne en CI sans dépendre de la disponibilité de l'ADEME.

---

## Limites connues

- **Le chargement initial prend ~5,5 h.** L'API ADEME ne propose pas de
  téléchargement en masse : les deux jeux exposés sont virtuels et n'ont pas de
  fichier source téléchargeable. La pagination est donc la seule voie.
- **Pas de SCD2 sur l'historique des DPE.** Le modèle conserve l'état courant.
  Suivre l'évolution d'un logement dans le temps demanderait des `snapshots` dbt
  sur `numero_dpe`, ce qui n'a d'intérêt qu'avec plusieurs mois de collecte.
- **Coordonnées en Lambert 93.** Power BI attend du WGS84 pour ses cartes ; la
  conversion est faite côté rapport plutôt que dans le pipeline.
- **Volumétrie réduite à 63 colonnes sur 230.** Les ~170 champs écartés
  détaillent les générateurs de chauffage et d'ECS, sans usage analytique ici.
  Le choix divise l'empreinte disque par quatre.

---

## Sources

- [Base DPE logements existants — ADEME](https://data.ademe.fr/datasets/dpe03existant)
- [Documentation de l'API Data Fair](https://data.ademe.fr/data-fair/api-doc)

Données publiées sous [Licence Ouverte / Open Licence](https://www.etalab.gouv.fr/licence-ouverte-open-licence).
