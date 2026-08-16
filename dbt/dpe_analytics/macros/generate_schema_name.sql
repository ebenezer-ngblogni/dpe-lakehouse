{#
    Par défaut, dbt préfixe le schéma cible au schéma personnalisé : un modèle
    configuré avec `+schema: marts` sur un profil pointant `public` atterrit
    dans `public_marts`.

    Ce comportement protège les environnements partagés, où chaque développeur
    travaille dans son propre préfixe. Ici il nuit : les droits accordés à
    `powerbi_reader` portent sur `marts` et `staging`, et l'initialisation de
    l'entrepôt crée ces schémas explicitement. Un modèle publié dans
    `public_marts` serait invisible pour Power BI.

    On retourne donc le schéma personnalisé tel quel, et on ne retombe sur le
    schéma cible que pour les modèles qui n'en déclarent aucun.
#}

{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}

{%- endmacro %}
