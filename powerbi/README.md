# Restitution Power BI

Ce dossier documente la couche de restitution : ce qui existe réellement, et
les contraintes qui en ont dicté la forme.

---

## Ce qui est publié

| Objet | Contenu |
|---|---|
| Modèle sémantique `DPE Départements France` | La table `Départements` (105 lignes), alimentée par `powerbi/data/mart_performance_departement.csv` |
| Rapport `Passoires thermiques par département` | Une page, quatre visuels, filtrée sur la France métropolitaine |

Les CSV de `powerbi/data/` sont versionnés volontairement : Power BI Service
les lit par leur URL brute GitHub. C'est le seul chemin d'ingestion disponible
ici, faute de licence OneDrive pour téléverser un fichier.

---

## La contrainte qui explique tout le reste

Le modèle sémantique est créé en **« création rapide »**, donc en lecture
seule : **aucune mesure DAX ne peut y être ajoutée**. L'édition du modèle
demande une capacité Fabric ou Premium, absente de ce compte.

Trois conséquences, toutes assumées dans la modélisation dbt :

1. **Les taux sont des colonnes pré-calculées**, pas des mesures. Ils sont
   calculés en SQL comme un rapport de sommes — `SUM(passoires) / SUM(dpe)` —
   jamais comme une moyenne de taux.

2. **Le grain de la table colle exactement à ce que le visuel affiche** : une
   ligne par département, toutes années confondues. Si la table avait gardé le
   grain département × année, Power BI aurait moyenné les taux annuels entre
   eux, ce qui n'a aucun sens dès que les dénominateurs diffèrent.

3. **Aucun indicateur de la page n'agrège un taux.** Les deux cartes affichent
   des comptages, qui se somment sans risque. Un indicateur « taux national »
   aurait exigé une mesure DAX : il est donc absent plutôt que faux. La valeur
   exacte, 8,69 %, figure dans le titre de la page.

---

## La page

**Filtre de page : `Métropole = t`.** Les DOM et collectivités sont écartés
pour deux raisons. D'abord la cohérence : leurs centroïdes sortent des bornes
de la carte métropolitaine. Ensuite la robustesse — Mayotte n'a que 11
diagnostics et la Nouvelle-Calédonie 10, si bien que leurs taux respectifs de
36 % et 20 % ne reposent que sur 4 et 2 logements. En métropole, le plus petit
département est la Lozère avec 5 698 diagnostics.

| Visuel | Ce qu'il montre |
|---|---|
| Deux cartes | 7 847 244 diagnostics, 681 719 passoires |
| Carte Azure Maps | 96 départements, taille du cercle = taux de passoires |
| Barres | Les 15 départements au taux le plus élevé |
| Nuage de points | Part de maisons contre taux de passoires, taille = volume |

### Pourquoi le grain départemental

La première version cartographiait les communes. À 23 628 communes
géolocalisées, chaque point occupe moins d'un pixel : les cercles se
recouvrent, plus rien n'est identifiable ni cliquable. Aucun réglage
d'affichage ne corrige cela — ni le rayon, ni l'opacité, ni le passage en
carte thermique, tous essayés. C'est le grain qui était en cause.

À 96 points, chaque bulle redevient lisible et porteuse de sens.

### Sur le nuage de points

Le titre annonce une corrélation faible, et c'est mesuré : **r = 0,31**, soit
9 % de variance expliquée. La part de maisons compte, mais elle ne suffit pas
à expliquer les écarts entre départements — le climat et la ruralité pèsent
davantage. Le titre le dit plutôt que de laisser croire à un lien fort.

À l'échelle du logement, en revanche, l'écart est net : 14,5 % de passoires
pour les maisons contre 6,2 % pour les appartements. C'est l'agrégation
départementale qui dilue l'effet.

---

## Reconstruire le modèle

1. **Créer** → *Modèle sémantique* → *CSV*
2. **Lien vers le fichier**, authentification *Anonyme*, URL :
   `https://raw.githubusercontent.com/ebenezer-ngblogni/dpe-lakehouse/main/powerbi/data/mart_performance_departement.csv`
3. **Délimiteur : Virgule.** Il n'est pas détecté automatiquement et tout
   atterrit sinon dans une colonne unique.
4. **Remplacer l'étape de typage** par une conversion explicite en locale
   `en-US`. C'est indispensable : le CSV utilise le point comme séparateur
   décimal, la locale française attend la virgule, et Power BI retombe alors
   sur du texte pour toutes les colonnes décimales — y compris la latitude et
   la longitude, ce qui vide la carte.

```m
Table.TransformColumnTypes(#"En-têtes promus",
  {{"code_departement", type text}, ..., {"latitude", type number}}, "en-US")
```

   Le code département doit rester du **texte** : typé en nombre, « 01 »
   perdrait son zéro initial. Le même piège existe côté dbt, où le seed
   `ref_departement` déclare ses types explicitement.

5. **Renommer les colonnes** en libellés lisibles via `Table.RenameColumns`.
6. Dans le visuel carte, **latitude et longitude doivent être en « Ne pas
   résumer »** — Power BI l'exige pour tracer des paires de coordonnées.

---

## Régénérer les données

```bash
make dbt-run
python scripts/export_powerbi.py \
  --table mart_performance_departement --table mart_profil_batiment \
  --destination powerbi/data
git add powerbi/data && git commit && git push
```

Le rapport lit l'URL GitHub : **il faut pousser pour que l'actualisation voie
les nouvelles données.**

La reprojection Lambert 93 → WGS84 est faite par le script d'export, avec un
garde-fou sur les bornes métropolitaines. Sur les 105 lignes exportées, 96
ressortent géolocalisées : exactement les départements métropolitains.

---

## Reste à faire

- `mart_profil_batiment` est exporté mais pas encore exploité. Il porte le
  résultat le plus démonstratif du jeu de données — la part de passoires passe
  de 17,5 % avant 1948 à 0,0 % après 2013, avec un décrochage net après la
  réglementation thermique de 1974. Il lui faut son propre modèle sémantique,
  chaque CSV en engendrant un.
- Le taux national ne peut pas être affiché en indicateur tant que le modèle
  reste en lecture seule.
