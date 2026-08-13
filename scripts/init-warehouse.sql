-- Initialisation de l'entrepôt analytique.
-- Exécuté une seule fois, au premier démarrage du conteneur PostgreSQL.

-- Schéma alimenté par Spark via JDBC : la donnée nettoyée, avant modélisation.
create schema if not exists silver;

-- Schémas construits par dbt.
create schema if not exists staging;
create schema if not exists marts;
create schema if not exists reference;

-- Rôle en lecture seule destiné à Power BI.
-- Un rapport n'a aucune raison de pouvoir écrire dans l'entrepôt : lui donner
-- le compte propriétaire est le raccourci qu'on regrette le jour où une
-- actualisation part de travers.
do $$
begin
  if not exists (select from pg_roles where rolname = 'powerbi_reader') then
    create role powerbi_reader login password 'powerbi';
  end if;
end
$$;

grant usage on schema marts, staging to powerbi_reader;
grant select on all tables in schema marts, staging to powerbi_reader;

-- Les modèles dbt sont recréés à chaque exécution : sans privilèges par défaut,
-- Power BI perdrait l'accès après le premier `dbt run`.
alter default privileges in schema marts grant select on tables to powerbi_reader;
alter default privileges in schema staging grant select on tables to powerbi_reader;
