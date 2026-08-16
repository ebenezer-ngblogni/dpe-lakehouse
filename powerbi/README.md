# Rapport Power BI — Performance énergétique du parc de logements

Ce dossier documente la couche restitution. Le fichier `.pbix` n'est pas
versionné (il embarque les données importées, plusieurs centaines de Mo) : ce
guide permet de le reconstruire à l'identique.

---

## 1. Connexion à l'entrepôt

**Obtenir les données → Base de données PostgreSQL**

| Champ | Valeur |
|---|---|
| Serveur | `localhost:5434` |
| Base de données | `dpe` |
| Mode | **Importer** |
| Utilisateur | `powerbi_reader` |
| Mot de passe | celui défini dans `scripts/init-warehouse.sql` |

Deux points importants :

- **Utiliser `powerbi_reader`, pas le compte propriétaire.** Ce rôle est en
  lecture seule sur les schémas `marts` et `staging`. Un rapport n'a aucune
  raison de pouvoir écrire dans l'entrepôt.
- **Mode Import et non DirectQuery.** En Import, le rapport reste consultable
  même quand la pile Docker est éteinte — indispensable pour une démonstration
  en entretien ou un partage sur LinkedIn. DirectQuery afficherait des visuels
  vides sans la base allumée.

Tables à charger : `marts.fct_dpe`, `marts.dim_commune`,
`marts.mart_performance_commune`.

> Sur un poste modeste, charger `mart_performance_commune` et `dim_commune`
> suffit pour l'essentiel du rapport : `fct_dpe` et ses 15 M de lignes ne sont
> nécessaires que pour les analyses au niveau du logement individuel.

---

## 2. Modèle de données

Schéma en étoile, relations à créer dans la vue *Modèle* :

```
        dim_commune
       (code_commune)
              │  1
              │
              │  ∗
          fct_dpe
       (code_commune)

  mart_performance_commune ── relié à dim_commune sur code_commune
```

- Cardinalité : **un-à-plusieurs**, `dim_commune` → `fct_dpe`
- Sens de filtrage : **simple**, de la dimension vers les faits
- Masquer `code_commune` côté `fct_dpe` pour éviter que l'utilisateur filtre sur
  la mauvaise colonne

### Conversion des coordonnées

Les coordonnées sont en **Lambert 93** (EPSG:2154), Power BI attend du **WGS84**
(latitude/longitude). La conversion se fait en Power Query, sur `dim_commune`.

L'approximation ci-dessous est suffisante pour une carte à l'échelle communale
(erreur de l'ordre de la centaine de mètres). Pour une précision métrique, il
faudrait faire la reprojection en amont, dans le pipeline.

```m
// Colonne personnalisée : latitude approchée depuis Lambert 93
let
    x = [centroide_x_lambert93],
    y = [centroide_y_lambert93]
in
    if x = null or y = null then null
    else 46.5 + (y - 6600000) / 111320
```

```m
// Colonne personnalisée : longitude approchée
let
    x = [centroide_x_lambert93],
    y = [centroide_y_lambert93],
    lat = 46.5 + (y - 6600000) / 111320
in
    if x = null or y = null then null
    else 3.0 + (x - 700000) / (111320 * Number.Cos(lat * Number.PI / 180))
```

Marquer ensuite les colonnes en *Catégorie de données → Latitude / Longitude*.

---

## 3. Mesures DAX

À créer dans une table de mesures dédiée (`Ruban → Saisir des données`, table
vide nommée `_Mesures`) : les regrouper évite qu'elles se dispersent dans les
tables de faits.

```dax
Nombre de DPE = COUNTROWS(fct_dpe)
```

```dax
Nombre de passoires =
CALCULATE(
    COUNTROWS(fct_dpe),
    fct_dpe[est_passoire_thermique] = TRUE()
)
```

```dax
-- DIVIDE plutôt que l'opérateur / : gère la division par zéro sans erreur
Taux de passoires % =
DIVIDE([Nombre de passoires], [Nombre de DPE], 0) * 100
```

```dax
Consommation moyenne EP =
AVERAGE(fct_dpe[conso_ep_kwh_m2_an])
```

```dax
-- La médiane résiste aux valeurs extrêmes que la moyenne subit
Consommation médiane EP =
MEDIAN(fct_dpe[conso_ep_kwh_m2_an])
```

```dax
Coût énergétique moyen =
AVERAGE(fct_dpe[cout_annuel_total_eur])
```

```dax
-- Évolution du taux de passoires par rapport à l'année précédente.
-- Sans table de dates dédiée, on décale sur la colonne d'année entière.
Taux passoires N-1 =
VAR AnneeCourante = SELECTEDVALUE(fct_dpe[annee_etablissement])
RETURN
    CALCULATE(
        [Taux de passoires %],
        fct_dpe[annee_etablissement] = AnneeCourante - 1
    )
```

```dax
Écart taux passoires =
VAR Precedent = [Taux passoires N-1]
RETURN IF(NOT ISBLANK(Precedent), [Taux de passoires %] - Precedent)
```

---

## 4. Pages du rapport

### Page 1 — Vue nationale

- **Cartes de synthèse** : nombre de DPE, taux de passoires, consommation
  médiane, coût énergétique moyen
- **Carte choroplèthe** par département, colorée sur `Taux de passoires %`
- **Histogramme empilé** : répartition A→G par année d'établissement
- **Segments** : année, type de bâtiment, tranche d'âge du bâti

Utiliser la palette réglementaire du DPE (vert A → rouge G) plutôt que les
couleurs par défaut : elle est immédiatement lisible par quiconque a déjà vu un
diagnostic.

### Page 2 — Analyse territoriale

- **Tableau** commune par commune : nombre de DPE, taux de passoires,
  consommation médiane, tri décroissant sur le taux
- **Nuage de points** : consommation médiane × taille du parc, un point par
  commune, taille selon `nb_dpe`
- **Drill-through** vers le détail d'une commune

### Page 3 — Déterminants de la performance

- **Matrice** tranche d'âge du bâti × étiquette DPE
- **Barres** : consommation moyenne par énergie de chauffage
- **Barres** : consommation moyenne par qualité d'isolation de l'enveloppe

C'est la page qui porte le message analytique : elle montre l'effet de la
réglementation thermique de 1974 sur la performance du parc.

### Page 4 — Qualité des données

Souvent négligée, c'est pourtant celle qui distingue un projet sérieux.

- Nombre de lignes rejetées par motif (depuis `silver.dpe_rejets`)
- Part des DPE au géocodage non fiable
- Complétude des colonnes clés

---

## 5. À versionner

```
powerbi/
├── README.md              ce guide
├── modele.bim             modèle exporté (Tabular Editor), sans données
└── captures/              copies d'écran pour le README et LinkedIn
```

Le `.pbix` est exclu par `.gitignore`. Pour partager le rapport lui-même,
publier sur Power BI Service et référencer le lien, ou exporter en PDF.
