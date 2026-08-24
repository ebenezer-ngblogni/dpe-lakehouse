-- Agrégat par département : c'est la table qui alimente la carte de France.
--
-- Pourquoi un grain départemental alors que `mart_performance_commune` existe
-- déjà ? Parce qu'une carte nationale à 23 628 communes géolocalisées est
-- illisible : chaque commune occupe moins d'un pixel, les bulles se recouvrent
-- et plus rien n'est cliquable. À 96 départements métropolitains, chaque point
-- redevient identifiable et porteur de sens.
--
-- Grain : un département, toutes années confondues. Ce choix n'est pas
-- cosmétique, il est imposé par la couche de restitution. Le modèle sémantique
-- Power BI est créé en « création rapide » et donc en lecture seule : aucune
-- mesure DAX ne peut y être ajoutée. Les taux doivent donc être des colonnes
-- pré-calculées, et le grain de la table doit coïncider exactement avec ce que
-- le visuel affiche. Sinon Power BI moyennerait des taux entre eux — un taux
-- moyen de taux n'a aucun sens dès que les dénominateurs diffèrent.
--
-- `mart_performance_commune` conserve le grain commune × année pour les
-- analyses fines et les séries temporelles.

{{ config(
    materialized='table',
    indexes=[{'columns': ['code_departement'], 'type': 'btree', 'unique': True}]
) }}

with faits as (

    select * from {{ ref('fct_dpe') }}

),

communes as (

    select * from {{ ref('dim_commune') }}

),

-- Centre géographique du département, approché par la moyenne des centroïdes
-- de ses communes, chaque commune comptant pour une.
--
-- La moyenne est calculée en Lambert 93 et non en latitude/longitude : c'est
-- une projection plane, donc moyenner ses coordonnées a un sens métrique. La
-- reprojection en WGS84 intervient plus tard, dans `scripts/export_powerbi.py`.
-- Moyenner directement des degrés donnerait un point légèrement décalé.
--
-- Chaque commune pèse pareil, volontairement : pondérer par le nombre de DPE
-- tirerait le point vers la plus grosse agglomération, alors qu'on cherche à
-- placer la bulle au centre du département.
centres as (

    select
        code_departement,
        count(*)                                   as nb_communes,
        avg(centroide_x_lambert93)                 as centroide_x_lambert93,
        avg(centroide_y_lambert93)                 as centroide_y_lambert93
    from communes
    where centroide_x_lambert93 is not null
    group by code_departement

),

agrege as (

    select
        c.code_departement,

        min(f.annee_etablissement)                          as annee_min,
        max(f.annee_etablissement)                          as annee_max,

        count(*)                                            as nb_dpe,
        count(*) filter (where f.est_passoire_thermique)    as nb_passoires,
        count(*) filter (where f.est_performant)            as nb_logements_performants,

        count(*) filter (where f.etiquette_dpe = 'A')       as nb_etiquette_a,
        count(*) filter (where f.etiquette_dpe = 'B')       as nb_etiquette_b,
        count(*) filter (where f.etiquette_dpe = 'C')       as nb_etiquette_c,
        count(*) filter (where f.etiquette_dpe = 'D')       as nb_etiquette_d,
        count(*) filter (where f.etiquette_dpe = 'E')       as nb_etiquette_e,
        count(*) filter (where f.etiquette_dpe = 'F')       as nb_etiquette_f,
        count(*) filter (where f.etiquette_dpe = 'G')       as nb_etiquette_g,

        count(*) filter (where f.type_batiment = 'maison')      as nb_maisons,
        count(*) filter (where f.type_batiment = 'appartement') as nb_appartements,

        -- Ces moyennes sont calculées sur les faits, jamais en moyennant les
        -- moyennes communales : une moyenne de moyennes donnerait autant de
        -- poids à une commune de 30 diagnostics qu'à une préfecture qui en
        -- compte 40 000.
        round(avg(f.conso_ep_kwh_m2_an)::numeric, 1)        as conso_ep_moyenne,
        round(percentile_cont(0.5) within group (
            order by f.conso_ep_kwh_m2_an
        )::numeric, 1)                                      as conso_ep_mediane,
        round(avg(f.emission_ges_kg_m2_an)::numeric, 1)     as emission_ges_moyenne,
        round(avg(f.surface_m2)::numeric, 1)                as surface_moyenne_m2,
        round(avg(f.cout_annuel_total_eur)::numeric, 0)     as cout_annuel_moyen_eur

    from faits f
    inner join communes c using (code_commune)
    group by c.code_departement

)

select
    a.code_departement,
    -- Le référentiel est un seed versionné : les codes INSEE bruts (« 05 »,
    -- « 2A ») ne parlent à personne sur une carte.
    --
    -- Une ligne reste non rattachée, pour un seul diagnostic sur 7,8 M : son
    -- code commune vaut littéralement « RENNES ». On la conserve plutôt que de
    -- la filtrer, pour que la somme des départements retombe sur le total
    -- national ; `est_metropole` valant faux, la carte l'écarte d'elle-même.
    case
        when r.nom_departement is not null   then r.nom_departement
        when a.code_departement is not null  then 'Inconnu (' || a.code_departement || ')'
        else 'Département non déterminé'
    end                                                                  as nom_departement,
    r.nom_region,
    coalesce(r.est_metropole, false)                                     as est_metropole,

    a.annee_min,
    a.annee_max,
    a.nb_dpe,
    a.nb_passoires,
    a.nb_logements_performants,
    a.nb_etiquette_a,
    a.nb_etiquette_b,
    a.nb_etiquette_c,
    a.nb_etiquette_d,
    a.nb_etiquette_e,
    a.nb_etiquette_f,
    a.nb_etiquette_g,
    a.nb_maisons,
    a.nb_appartements,

    -- Indicateurs de la carte. Rapport de sommes, jamais moyenne de taux.
    round(100.0 * a.nb_passoires / nullif(a.nb_dpe, 0), 2)               as taux_passoires_pct,
    round(100.0 * a.nb_logements_performants / nullif(a.nb_dpe, 0), 2)   as taux_performants_pct,
    round(100.0 * a.nb_maisons / nullif(a.nb_dpe, 0), 2)                 as part_maisons_pct,

    a.conso_ep_moyenne,
    a.conso_ep_mediane,
    a.emission_ges_moyenne,
    a.surface_moyenne_m2,
    a.cout_annuel_moyen_eur,
    -- Le coût au m² neutralise l'effet de taille : sans lui, un département de
    -- grandes maisons paraît toujours plus cher qu'un département d'petits
    -- appartements, à performance énergétique identique.
    round(a.cout_annuel_moyen_eur / nullif(a.surface_moyenne_m2, 0), 1)  as cout_annuel_par_m2_eur,

    ctr.nb_communes,
    ctr.centroide_x_lambert93,
    ctr.centroide_y_lambert93

from agrege a
left join centres ctr using (code_departement)
left join {{ ref('ref_departement') }} r using (code_departement)
