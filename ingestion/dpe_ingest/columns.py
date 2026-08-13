"""Colonnes retenues sur les 230 exposées par l'API ADEME.

Le choix est volontairement restrictif : 63 champs couvrant l'identité, la
géographie, le bâti, la performance et les équipements. Les ~170 champs écartés
décrivent le détail des générateurs (n1/n2/n3 de chauffage et d'ECS), utile au
diagnostiqueur mais pas à l'analyse agrégée que sert ce projet.

Coût du choix : ~2,5 Go en Parquet au lieu de ~10 Go.
"""

# Identité du DPE et cycle de vie du document
IDENTITE = [
    "numero_dpe",
    "numero_dpe_remplace",
    "numero_dpe_immeuble_associe",
    "date_etablissement_dpe",
    "date_reception_dpe",
    "date_visite_diagnostiqueur",
    "date_fin_validite_dpe",
    "date_derniere_modification_dpe",
    "version_dpe",
    "modele_dpe",
    "methode_application_dpe",
]

# Adressage et géocodage (BAN = Base Adresse Nationale, RNB = Référentiel National des Bâtiments)
GEOGRAPHIE = [
    "code_insee_ban",
    "code_postal_ban",
    "nom_commune_ban",
    "code_departement_ban",
    "code_region_ban",
    "adresse_ban",
    "identifiant_ban",
    "coordonnee_cartographique_x_ban",
    "coordonnee_cartographique_y_ban",
    "score_ban",
    "statut_geocodage",
    "id_rnb",
]

# Caractéristiques physiques du logement / de l'immeuble
BATIMENT = [
    "type_batiment",
    "annee_construction",
    "periode_construction",
    "surface_habitable_logement",
    "surface_habitable_immeuble",
    "nombre_niveau_logement",
    "nombre_appartement",
    "typologie_logement",
    "hauteur_sous_plafond",
    "classe_altitude",
    "zone_climatique",
    "classe_inertie_batiment",
]

# Étiquettes réglementaires et indicateurs normalisés au m²
PERFORMANCE = [
    "etiquette_dpe",
    "etiquette_ges",
    "conso_5_usages_par_m2_ep",
    "conso_5_usages_par_m2_ef",
    "emission_ges_5_usages_par_m2",
    "ubat_w_par_m2_k",
]

# Consommations, émissions et coûts en valeur absolue
CONSOMMATION = [
    "conso_5_usages_ep",
    "conso_5_usages_ef",
    "conso_chauffage_ef",
    "conso_ecs_ef",
    "emission_ges_5_usages",
    "cout_total_5_usages",
    "cout_chauffage",
    "cout_ecs",
    "besoin_chauffage",
    "besoin_ecs",
]

# Vecteurs énergétiques et équipements
EQUIPEMENTS = [
    "type_energie_principale_chauffage",
    "type_generateur_chauffage_principal",
    "type_installation_chauffage",
    "type_energie_principale_ecs",
    "type_ventilation",
    "type_generateur_froid",
    "presence_production_pv",
    "production_electricite_pv_kwhep_par_an",
]

# Qualité de l'enveloppe thermique
ISOLATION = [
    "qualite_isolation_enveloppe",
    "qualite_isolation_murs",
    "qualite_isolation_menuiseries",
    "qualite_isolation_plancher_bas",
    "deperditions_enveloppe",
    "deperditions_murs",
    "deperditions_baies_vitrees",
]

SELECTED_COLUMNS: list[str] = (
    IDENTITE + GEOGRAPHIE + BATIMENT + PERFORMANCE + CONSOMMATION + EQUIPEMENTS + ISOLATION
)

# Clé primaire métier et clé de partitionnement du backfill
PRIMARY_KEY = "numero_dpe"
PARTITION_DATE_FIELD = "date_etablissement_dpe"

# Champ de watermark pour les runs incrémentaux : l'ADEME corrige des DPE
# a posteriori, donc on suit la dernière modification et non la date d'établissement.
WATERMARK_FIELD = "date_derniere_modification_dpe"


def select_clause() -> str:
    """Valeur du paramètre `select` de l'API Data Fair."""
    return ",".join(SELECTED_COLUMNS)
