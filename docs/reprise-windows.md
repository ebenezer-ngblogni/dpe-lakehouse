# Reprise du projet depuis Windows

Document de passation. À coller dans une nouvelle conversation Claude Desktop
pour qu'elle démarre avec le contexte, ou à lire seul.

---

## Le projet en dix lignes

**DPE Lakehouse** — pipeline de bout en bout sur la base ADEME des diagnostics
de performance énergétique des logements.

- **Source** : API ADEME Data Fair, jeu `dpe03existant`, accès libre
- **Volumes** : 15,3 M de lignes ingérées, 8 004 187 en entrepôt après le
  dernier run
- **Chaîne** : ingestion Python → nettoyage Spark/Scala → PostgreSQL →
  modèles dbt → orchestration Airflow → restitution Power BI
- **Dépôt** : `github.com/ebenezer-ngblogni/dpe-lakehouse`

Architecture médaillon : bronze (brut, jamais modifié), silver (nettoyé,
dédoublonné, règles qualité), gold (marts agrégés pour la restitution).

---

## Ce qui est fait, ce qui reste

### Fait

- Ingestion idempotente : partitions mensuelles, promotion atomique, manifeste
  de complétude
- Job Spark en Scala : déduplication par fenêtre, résolution des chaînes de
  remplacement, règles qualité avec motif de rejet conservé
- Entrepôt PostgreSQL : 40 colonnes projetées sur 78, fenêtre depuis 2024
- Modèles dbt : schéma en étoile, agrégats commune et département, 44 tests
- DAG Airflow à six tâches, exécuté de bout en bout avec succès
  (6 min + 18 min + 4 min + 3 min)
- Rapport Power BI Service : carte à 96 départements, classement, nuage de points
- Deux pages web, un canevas d'architecture en PNG, un tutoriel PDF de 16 pages

### En suspens

1. **`dbt_test` bloque le DAG.** Deux tests échouent sur un vrai doublon de la
   source (l'ADEME a publié deux fois le même diagnostic, version incomplète
   puis complète). Avec `retries: 2`, la tâche finit par échouer et
   `dbt_docs_generate` ne tourne jamais. **Décision à prendre** : passer ces
   deux tests en `severity: warn` avec commentaire, ou laisser rouge.
2. **Le job Scala de la CI** échoue à l'étape « Set up job ». Le log demande
   les droits admin du dépôt.
3. **`mart_profil_batiment`** est exporté mais pas encore exploité dans un
   rapport. C'est lui qui porte le résultat le plus démonstratif.

---

## Faire du Power BI Desktop sous Windows

Le poste est en double amorçage : quand Windows tourne, la pile Docker Linux
est éteinte, donc **PostgreSQL est injoignable**. Deux chemins possibles.

### Chemin 1 — Les CSV publiés sur GitHub (recommandé)

Vérifié accessible : les quatre fichiers répondent en HTTP 200.

```
https://raw.githubusercontent.com/ebenezer-ngblogni/dpe-lakehouse/main/powerbi/data/mart_performance_departement.csv
https://raw.githubusercontent.com/ebenezer-ngblogni/dpe-lakehouse/main/powerbi/data/mart_performance_commune.csv
https://raw.githubusercontent.com/ebenezer-ngblogni/dpe-lakehouse/main/powerbi/data/dim_commune.csv
https://raw.githubusercontent.com/ebenezer-ngblogni/dpe-lakehouse/main/powerbi/data/mart_profil_batiment.csv
```

Dans Power BI Desktop : **Obtenir les données → Web**, coller l'URL.

### Chemin 2 — La partition partagée

La partition `sda5` (« Nouveau nom », 1 To) est en **NTFS**, donc Windows la
monte nativement. Les exports lourds s'y trouvent :

```
<lettre>:\dpelab\data\exports\powerbi\
```

C'est le seul chemin pour `fct_dpe.parquet` (266 Mo, 7,8 M de lignes), trop
lourd pour être versionné.

Attention : `/home/eben/Bureau/lab/DataEng/` est sur une partition **ext4**,
que Windows ne lit pas nativement. Les CSV du dépôt ne sont donc pas
accessibles par ce chemin — il faut passer par GitHub.

### Les deux pièges Power BI, silencieux tous les deux

1. **Le délimiteur n'est pas détecté** : tout atterrit dans une colonne
   unique. Le forcer à « Virgule ».
2. **L'auto-typage en locale française casse les décimaux** : le CSV écrit
   `8.69` avec un point, la locale attend une virgule, et Power Query bascule
   la colonne en texte — latitude et longitude comprises, ce qui vide la carte
   *sans message d'erreur*.

Remplacer l'étape de typage par :

```
Table.TransformColumnTypes(#"En-têtes promus",
  {{"code_departement", type text},
   {"nb_dpe", Int64.Type},
   {"taux_passoires_pct", type number},
   {"latitude", type number}, {"longitude", type number}}, "en-US")
```

Le code département doit rester **texte** : typé en nombre, « 01 » perdrait
son zéro initial et « 2A » ferait échouer la conversion.

### Ce que Desktop permet et que le Service ne permettait pas

Le modèle sémantique créé en « création rapide » dans Power BI Service est en
**lecture seule** : aucune mesure DAX possible. C'est pour cela que les taux
sont des colonnes pré-calculées en SQL.

**Sous Desktop, cette contrainte disparaît.** On peut enfin écrire :

```dax
Taux de passoires =
DIVIDE(SUM(Départements[Passoires thermiques]), SUM(Départements[Nombre de DPE]))
```

C'est un rapport de sommes, donc juste quel que soit le niveau d'agrégation —
contrairement à `AVERAGE(taux)`, qui donnerait le même poids à un département
de 5 698 diagnostics qu'à un de 504 567.

---

## Les chiffres à connaître

| Résultat | Valeur |
|---|---|
| Part de passoires (F ou G) | **8,71 %** sur 8 002 235 diagnostics métropolitains |
| Avant 1948 → après 2013 | **17,5 % → 0,0 %** de passoires |
| Maisons neuves depuis 2013 | **60,3 %** classées A ou B |
| Écart Creuse / Hérault | **34,6 % contre 2,1 %**, facteur 17 |
| Paris | 15,7 %, seul département urbain dense du peloton de tête |
| Coût de chauffage | **6 € contre 50 €** du m² par an, étiquette A contre G |
| Maison contre appartement | 14,5 % contre 6,2 % de passoires |
| Corrélation part de maisons / taux | **r = 0,31** — lien réel mais modeste |

### Les limites à énoncer soi-même

- **Biais de sélection** : un DPE se fait à la vente ou à la location. Ces
  8 millions de lignes décrivent le parc *qui change de mains*, pas le parc
  français.
- **Fenêtre temporelle** : l'entrepôt ne contient que 2024 et après. Le lac
  couvre depuis juillet 2021.

---

## Relancer la pile côté Linux

```bash
cd ~/Bureau/lab/DataEng
udisksctl mount -b /dev/sda5          # le lac, si `data` est un lien mort
export AIRFLOW_UID=$(id -u) HOST_UID=$(id -u)
export DOCKER_GID=$(getent group docker | cut -d: -f3)
make up
```

Interfaces : Airflow `localhost:8080` (admin/admin), MinIO `localhost:9003`,
PostgreSQL `localhost:5434`, dbt docs `localhost:8081` après
`dbt docs serve --profiles-dir . --port 8081`.

**Piège connu** : si le scheduler Airflow ne sérialise pas le DAG, c'est un
problème de droits sur le volume de logs. Correction :

```bash
docker exec -u 0 dpe_airflow_scheduler chown -R "$(id -u):0" /opt/airflow/logs
docker restart dpe_airflow_scheduler
```
