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
    -- géocodage est fiable : inclure les adresses mal rapprochées décalerait
    -- le point vers le centre du département.
    select
        code_commune,
        avg(coord_x_lambert93) as centroide_x_lambert93,
        avg(coord_y_lambert93) as centroide_y_lambert93,
        count(*)               as nb_points_fiables
    from dpe
    where geocodage_fiable
      and coord_x_lambert93 is not null
      and coord_y_lambert93 is not null
    group by code_commune

)

select
    l.code_commune,
    l.nom_commune,
    l.code_departement,
    l.code_region,
    c.centroide_x_lambert93,
    c.centroide_y_lambert93,
    coalesce(c.nb_points_fiables, 0) as nb_points_geocodes_fiables
from libelles_classes l
left join centroides c using (code_commune)
where l.rang_libelle = 1
