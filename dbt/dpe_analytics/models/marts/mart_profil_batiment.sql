-- Performance énergétique croisée par type de logement et période de
-- construction.
--
-- C'est la table qui porte le résultat le plus parlant du jeu de données : la
-- part de passoires s'effondre après 1974, année de la première réglementation
-- thermique adoptée dans la foulée du choc pétrolier. L'effet d'une politique
-- publique devient directement lisible sur des données de terrain.
--
-- Grain : type de bâtiment × tranche d'âge. Une quinzaine de lignes, taux
-- pré-calculés — même contrainte que `mart_performance_departement` : le
-- modèle sémantique Power BI est en lecture seule, les taux ne peuvent pas
-- être des mesures DAX.

{{ config(materialized='table') }}

with faits as (

    select * from {{ ref('fct_dpe') }}
    where type_batiment is not null
      and tranche_age_batiment is not null

),

agrege as (

    select
        type_batiment,
        tranche_age_batiment,

        count(*)                                            as nb_dpe,
        count(*) filter (where est_passoire_thermique)      as nb_passoires,
        count(*) filter (where est_performant)              as nb_logements_performants,

        round(avg(conso_ep_kwh_m2_an)::numeric, 1)          as conso_ep_moyenne,
        round(avg(emission_ges_kg_m2_an)::numeric, 1)       as emission_ges_moyenne,
        round(avg(surface_m2)::numeric, 1)                  as surface_moyenne_m2,
        round(avg(cout_annuel_total_eur)::numeric, 0)       as cout_annuel_moyen_eur

    from faits
    group by type_batiment, tranche_age_batiment

)

select
    type_batiment,
    tranche_age_batiment,

    -- Libellé d'affichage : la couche Spark produit des valeurs sans accent
    -- (« 2013 et apres »). On les corrige ici plutôt qu'en amont, car c'est
    -- une question de présentation, pas de donnée.
    case tranche_age_batiment
        when '2013 et apres' then '2013 et après'
        when 'inconnue'      then 'Période inconnue'
        else tranche_age_batiment
    end                                                     as periode_construction,

    -- Rang d'affichage : trié alphabétiquement, « avant 1948 » se retrouverait
    -- après « 2013 et après », ce qui casserait la lecture chronologique du
    -- graphique. Power BI trie sur cette colonne plutôt que sur le libellé.
    case tranche_age_batiment
        when 'avant 1948'    then 1
        when '1948-1974'     then 2
        when '1975-1988'     then 3
        when '1989-2000'     then 4
        when '2001-2012'     then 5
        when '2013 et apres' then 6
        else 99
    end                                                     as ordre_periode,

    nb_dpe,
    nb_passoires,
    nb_logements_performants,
    round(100.0 * nb_passoires / nullif(nb_dpe, 0), 2)               as taux_passoires_pct,
    round(100.0 * nb_logements_performants / nullif(nb_dpe, 0), 2)   as taux_performants_pct,

    conso_ep_moyenne,
    emission_ges_moyenne,
    surface_moyenne_m2,
    cout_annuel_moyen_eur,
    round(cout_annuel_moyen_eur / nullif(surface_moyenne_m2, 0), 1)  as cout_annuel_par_m2_eur

from agrege
