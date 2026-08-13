-- Façade sur la table silver : renommage vers les conventions de l'entrepôt,
-- aucune logique métier. Toute la préparation lourde (déduplication, chaînes de
-- remplacement, règles de qualité) a déjà été faite en Scala/Spark, là où elle
-- s'exécute sur 15 M de lignes sans saturer l'entrepôt.

with source as (

    select * from {{ source('silver', 'dpe_courant') }}

),

renomme as (

    select
        -- Identité
        numero_dpe                                  as cle_dpe,
        numero_dpe_immeuble_associe                 as cle_dpe_immeuble,

        -- Temps
        date_etablissement_dpe                      as date_etablissement,
        date_fin_validite_dpe                       as date_fin_validite,
        annee_etablissement,
        mois_etablissement,
        dpe_encore_valide,

        -- Géographie
        code_insee_ban                              as code_commune,
        nom_commune_ban                             as nom_commune,
        code_postal_ban                             as code_postal,
        code_departement_ban                        as code_departement,
        code_region_ban                             as code_region,
        coordonnee_cartographique_x_ban             as coord_x_lambert93,
        coordonnee_cartographique_y_ban             as coord_y_lambert93,
        geocodage_fiable,

        -- Bâti
        type_batiment,
        annee_construction,
        tranche_age_batiment,
        surface_habitable_logement                  as surface_m2,
        tranche_surface,
        nombre_niveau_logement                      as nombre_niveaux,

        -- Performance
        etiquette_dpe,
        etiquette_ges,
        est_passoire_thermique,
        est_performant,
        conso_5_usages_par_m2_ep                    as conso_ep_kwh_m2_an,
        conso_5_usages_par_m2_ef                    as conso_ef_kwh_m2_an,
        emission_ges_5_usages_par_m2                as emission_ges_kg_m2_an,
        ubat_w_par_m2_k                             as coefficient_ubat,

        -- Coûts annuels
        cout_total_5_usages                         as cout_annuel_total_eur,
        cout_chauffage                              as cout_annuel_chauffage_eur,
        cout_ecs                                    as cout_annuel_ecs_eur,

        -- Équipements
        type_energie_principale_chauffage           as energie_chauffage,
        type_generateur_chauffage_principal         as generateur_chauffage,
        type_energie_principale_ecs                 as energie_ecs,
        type_ventilation,

        -- Isolation
        qualite_isolation_enveloppe,
        qualite_isolation_murs,
        qualite_isolation_menuiseries

    from source

)

select * from renomme
