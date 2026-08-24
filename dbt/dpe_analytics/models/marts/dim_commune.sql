-- Dimension commune du modèle en étoile.
--
-- La source ne fournit pas de référentiel communal : on le dérive des DPE
-- eux-mêmes. Une commune peut apparaître sous plusieurs libellés selon la
-- qualité du géocodage, d'où le choix du libellé le plus fréquent plutôt que
-- d'un `max()` arbitraire.

{{ config(materialized='table') }}

with dpe as (

    select * from {{ ref('stg_dpe') }}
    where code_commune is not null

),

libelles_classes as (

    select
        code_commune,
        nom_commune,
        code_departement,
        code_region,
        count(*) as occurrences,
        row_number() over (
            partition by code_commune
            order by count(*) desc, nom_commune
        ) as rang_libelle
    from dpe
    group by code_commune, nom_commune, code_departement, code_region

),

centroides as (

    -- Centroïde approché de la commune, calculé sur les seuls DPE dont le
    -- géocodage est précis (adresse rapprochée de la BAN *et* score au-dessus
    -- de la médiane). Inclure les adresses mal rapprochées décalerait le point
    -- vers le centre du département.
    select
        code_commune,
        avg(coord_x_lambert93) as centroide_x_lambert93,
        avg(coord_y_lambert93) as centroide_y_lambert93,
        count(*)               as nb_points_precis
    from dpe
    where geocodage_precis
      and coord_x_lambert93 is not null
      and coord_y_lambert93 is not null
    group by code_commune

),

-- Rattrapage du code département quand la source ne le renseigne pas.
--
-- 85 communes sont dans ce cas, pour 2 695 diagnostics. Deux causes :
--
--  1. Le code commune est en réalité un code postal (« 02700 » pour Tergnier,
--     dont le code INSEE est 02738). Sans conséquence ici : en France, les
--     deux premiers caractères désignent le département dans les deux
--     nomenclatures.
--  2. La source préfixe « old » les communes fusionnées — « old05061 » est
--     l'ancien code de Gap. Le code réel suit le préfixe, il suffit de le
--     retirer.
--
-- La dérivation a été validée avant d'être appliquée : sur les 34 594 communes
-- où la source fournit un code département, elle retombe sur la même valeur
-- dans 100 % des cas. Elle n'est donc utilisée qu'en repli, jamais en
-- remplacement.
--
-- Les cas non résolus restent nuls plutôt que d'être devinés : « 20 » ne
-- permet pas de trancher entre Corse-du-Sud et Haute-Corse, et une ligne dont
-- le code commune vaut « RENNES » n'a rien d'exploitable.
code_repare as (

    select
        l.*,
        regexp_replace(l.code_commune, '^old\s*', '') as code_normalise
    from libelles_classes l
    where l.rang_libelle = 1

)

select
    l.code_commune,
    l.nom_commune,
    coalesce(
        l.code_departement,
        case
            when l.code_normalise ~ '^9[78][0-9]' then left(l.code_normalise, 3)
            when l.code_normalise ~ '^2[AB]'      then left(l.code_normalise, 2)
            when l.code_normalise ~ '^(0[1-9]|1[0-9]|2[1-9]|[3-8][0-9]|9[0-5])'
                                                  then left(l.code_normalise, 2)
        end
    ) as code_departement,
    l.code_region,
    c.centroide_x_lambert93,
    c.centroide_y_lambert93,
    coalesce(c.nb_points_precis, 0) as nb_points_geocodes_precis
from code_repare l
left join centroides c using (code_commune)
