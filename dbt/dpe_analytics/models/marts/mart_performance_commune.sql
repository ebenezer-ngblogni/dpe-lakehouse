-- Agrégat par commune et par année : c'est la table que Power BI interroge pour
-- la carte de France et les séries temporelles.
--
-- Pré-agréger ici plutôt que laisser Power BI agréger 15 M de lignes fait la
-- différence entre un rapport qui répond instantanément et un rapport qui rame.

{{ config(
    materialized='table',
    indexes=[{'columns': ['code_commune', 'annee_etablissement'], 'type': 'btree'}]
) }}

with faits as (

    select * from {{ ref('fct_dpe') }}

),

agrege as (

    select
        code_commune,
        annee_etablissement,

        count(*)                                            as nb_dpe,
        count(*) filter (where est_passoire_thermique)      as nb_passoires,
        count(*) filter (where est_performant)              as nb_logements_performants,

        -- Répartition détaillée par étiquette : permet un histogramme empilé
        -- dans Power BI sans avoir à recharger le détail.
        count(*) filter (where etiquette_dpe = 'A')         as nb_etiquette_a,
        count(*) filter (where etiquette_dpe = 'B')         as nb_etiquette_b,
        count(*) filter (where etiquette_dpe = 'C')         as nb_etiquette_c,
        count(*) filter (where etiquette_dpe = 'D')         as nb_etiquette_d,
        count(*) filter (where etiquette_dpe = 'E')         as nb_etiquette_e,
        count(*) filter (where etiquette_dpe = 'F')         as nb_etiquette_f,
        count(*) filter (where etiquette_dpe = 'G')         as nb_etiquette_g,

        round(avg(conso_ep_kwh_m2_an)::numeric, 1)          as conso_ep_moyenne,
        -- La médiane résiste aux valeurs extrêmes que la moyenne subit :
        -- quelques logements très énergivores suffisent à décaler la moyenne
        -- d'une petite commune.
        round(percentile_cont(0.5) within group (
            order by conso_ep_kwh_m2_an
        )::numeric, 1)                                      as conso_ep_mediane,

        round(avg(emission_ges_kg_m2_an)::numeric, 1)       as emission_ges_moyenne,
        round(avg(surface_m2)::numeric, 1)                  as surface_moyenne_m2,
        round(avg(cout_annuel_total_eur)::numeric, 0)       as cout_annuel_moyen_eur

    from faits
    group by code_commune, annee_etablissement

)

select
    a.*,
    -- Indicateur phare du rapport : part des passoires thermiques.
    round(100.0 * a.nb_passoires / nullif(a.nb_dpe, 0), 2) as taux_passoires_pct,
    round(100.0 * a.nb_logements_performants / nullif(a.nb_dpe, 0), 2) as taux_performants_pct,
    c.nom_commune,
    c.code_departement,
    c.code_region,
    c.centroide_x_lambert93,
    c.centroide_y_lambert93
from agrege a
left join {{ ref('dim_commune') }} c using (code_commune)
