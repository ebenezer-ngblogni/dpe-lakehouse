-- Table de faits du modèle en étoile. Grain : un diagnostic de performance
-- énergétique courant et valide.
--
-- Les attributs descriptifs à faible cardinalité (étiquette, tranche de
-- surface, type d'énergie) restent dans la table de faits comme dimensions
-- dégénérées : sortir chacun dans sa propre table imposerait à Power BI six
-- jointures supplémentaires pour un gain de stockage nul à cette cardinalité.

{{ config(
    materialized='table',
    indexes=[
      {'columns': ['code_commune'], 'type': 'btree'},
      {'columns': ['annee_etablissement'], 'type': 'btree'},
      {'columns': ['etiquette_dpe'], 'type': 'btree'}
    ]
) }}

select
    -- Clés
    cle_dpe,
    code_commune,
    annee_etablissement,
    mois_etablissement,
    date_etablissement,

    -- Dimensions dégénérées
    etiquette_dpe,
    etiquette_ges,
    type_batiment,
    tranche_age_batiment,
    tranche_surface,
    energie_chauffage,
    energie_ecs,
    type_ventilation,
    qualite_isolation_enveloppe,

    -- Drapeaux
    est_passoire_thermique,
    est_performant,
    dpe_encore_valide,
    geocodage_fiable,

    -- Mesures
    surface_m2,
    conso_ep_kwh_m2_an,
    conso_ef_kwh_m2_an,
    emission_ges_kg_m2_an,
    coefficient_ubat,
    cout_annuel_total_eur,
    cout_annuel_chauffage_eur,
    cout_annuel_ecs_eur,

    -- Mesure dérivée : consommation absolue du logement, utile pour agréger un
    -- parc immobilier là où la valeur au m² ne s'additionne pas.
    case
        when surface_m2 is not null and conso_ep_kwh_m2_an is not null
        then round((surface_m2 * conso_ep_kwh_m2_an)::numeric, 0)
    end as conso_ep_totale_kwh_an,

    annee_construction

from {{ ref('stg_dpe') }}
