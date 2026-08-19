# Données publiées pour le rapport

Ces deux exports agrégés sont versionnés — contrairement au lac, qui ne l'est
jamais — pour deux raisons :

1. **Reproductibilité.** N'importe qui peut rejouer le rapport Power BI sans
   dérouler toute la chaîne (ingestion de 15,3 M de lignes, Spark, PostgreSQL).
2. **Contrainte de licence.** Power BI Service n'accepte le téléversement de
   fichiers que via OneDrive Entreprise. Un compte sans cette licence doit
   passer par le connecteur « Lien vers le fichier », qui exige une URL
   publique — d'où leur présence ici.

| Fichier | Grain | Lignes |
|---|---|---|
| `mart_performance_commune.csv` | commune × année | 97 763 |
| `dim_commune.csv` | commune | 34 679 |

Ils sont régénérés par `make powerbi-export`, qui exporte depuis le schéma
`marts` de l'entrepôt. La table de faits (7,8 M de lignes) n'est pas versionnée :
elle reste dans `data/exports/powerbi/` en Parquet.

## URL brutes, à coller dans le connecteur Texte/CSV

```
https://raw.githubusercontent.com/ebenezer-ngblogni/dpe-lakehouse/main/powerbi/data/mart_performance_commune.csv
https://raw.githubusercontent.com/ebenezer-ngblogni/dpe-lakehouse/main/powerbi/data/dim_commune.csv
```
