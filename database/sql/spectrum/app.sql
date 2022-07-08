create table spectrum_country_mapping
(
  cov_spectrum_country text,
  cov_spectrum_region text,
  gisaid_country text,
  owid_country text
);

create table spectrum_region
(
  name text primary key,
  url_component text unique not null
);


insert into spectrum_region (name, url_component)
values
  ('Africa', 'africa'),
  ('Asia', 'asia'),
  ('Europe', 'europe'),
  ('North America', 'north-america'),
  ('South America', 'south-america'),
  ('Oceania', 'oceania');


create table spectrum_country
(
  iso_code text primary key
    references country (iso_code),
  name text unique not null,
  region text not null
    references spectrum_region (name),
  url_component text unique not null
);

insert into spectrum_country (iso_code, name, region, url_component)
select
  c.iso_code,
  c.name_english,
  c.region,
  lower(regexp_replace(c.name_english, '[ '']', '-', 'g')) as url_component
from
  country c
where
  c.iso_code <> 'XXX'
  and exists(
    select *
    from gisaid_sequence gs
    where gs.iso_country = c.iso_code
  );


create table spectrum_account (
  username text primary key,
  password_hash text not null,
  full_name text
);


create table spectrum_pangolin_lineage_recent_metrics (
  id serial primary key,
  insertion_timestamp timestamp not null,
  pangolin_lineage text not null,
  region text,
  country text,
  fitness_advantage float not null,
  fitness_advantage_lower float not null,
  fitness_advantage_upper float not null
);


create table spectrum_new_interesting_variant (
  id serial primary key,
  insertion_timestamp timestamp not null,
  country text not null
    references spectrum_country (name),
  data_type jsonb,
  result text not null
);

create index on spectrum_new_interesting_variant (insertion_timestamp);

create index on spectrum_new_interesting_variant (country);

create index on spectrum_new_interesting_variant (data_type);


create table spectrum_api_usage_sample
(
  id serial primary key,
  isoyear integer not null,
  isoweek integer not null,
  usage_count integer not null,
  fields text not null,
  private_version boolean not null,
  region text not null,
  country text not null,
  mutations text not null,
  match_percentage float not null,
  pangolin_lineage text not null,
  data_type text not null,
  date_from date not null,
  date_to date not null
);

create unique index spectrum_api_usage_sample_unique_index on spectrum_api_usage_sample
  (isoyear, isoweek, fields, private_version, region, country, mutations, match_percentage, pangolin_lineage, data_type, date_from, date_to);

alter table spectrum_api_usage_sample
add constraint spectrum_api_usage_sample_unique_constraint
unique using index spectrum_api_usage_sample_unique_index;


create table spectrum_api_cache_sample
(
  id serial primary key,
  fields text not null,
  private_version boolean not null,
  region text not null,
  country text not null,
  mutations text not null,
  match_percentage float not null,
  pangolin_lineage text not null,
  data_type text not null,
  date_from date not null,
  date_to date not null,
  cache text not null
);

create unique index on spectrum_api_cache_sample
  (fields, private_version, region, country, mutations, match_percentage, pangolin_lineage, data_type, date_from, date_to);


create table spectrum_waste_water_result
(
	variant_name text not null,
	location text not null,
	data jsonb not null,
	primary key (variant_name, location)
);


create table spectrum_waste_water_variant_text
(
  variant_name text primary key,
  description text not null
);


create table spectrum_usage_geo (
  id bigserial primary key,
  date date not null,
  country text,
  division text,
  city text,
  visitors integer not null,
  hits integer not null,
  bytes bigint not null
);

create table spectrum_usage_referrer (
  id bigserial primary key,
  date date not null,
  referring_site text,
  visitors integer not null,
  hits integer not null,
  bytes bigint not null
);

create table spectrum_usage_os (
  id bigserial primary key,
  date date not null,
  os_type text,
  os_exact text,
  visitors integer not null,
  hits integer not null,
  bytes bigint not null
);

create table spectrum_usage_browser (
  id bigserial primary key,
  date date not null,
  browser_type text,
  browser_exact text,
  visitors integer not null,
  hits integer not null,
  bytes bigint not null
);

create table spectrum_usage_hour (
  id bigserial primary key,
  date date not null,
  hour integer,
  visitors integer not null,
  hits integer not null,
  bytes bigint not null
);

create table spectrum_owid_global_cases_raw (
  iso_country text not null,
  region text not null,
  country text,
  date date not null,
  new_cases_per_million float,
  new_deaths_per_million float,
  new_cases int,
  new_deaths int,
  primary key (country, date)
);

create index on spectrum_owid_global_cases_raw (region);
create index on spectrum_owid_global_cases_raw (country);

create table spectrum_collection (
  id serial primary key,
  title text not null,
  description text not null,
  maintainers text not null,
  email text not null,
  admin_key text not null
);

create table spectrum_collection_variant (
  id serial primary key,
  collection_id integer references spectrum_collection on update cascade on delete cascade,
  query text not null,
  name text not null,
  description text not null
);

create index on spectrum_collection_variant (collection_id);

-- Only use OWID data
create view spectrum_cases as
select
  scm.cov_spectrum_region as region,
  scm.cov_spectrum_country as country,
  null as division,
  so.date,
  coalesce(so.new_cases, 0) as new_cases,
  coalesce(so.new_deaths, 0) as new_deaths
from
  spectrum_owid_global_cases_raw so
  join spectrum_country_mapping scm on so.country = scm.owid_country;

-- Using FOPH data for CH
-- create view spectrum_cases as
-- select  -- Countries other than Switzerland -> OWID
--   scm.cov_spectrum_region as region,
--   scm.cov_spectrum_country as country,
--   null as division,
--   so.date,
--   coalesce(so.new_cases, 0) as new_cases,
--   coalesce(so.new_deaths, 0) as new_deaths
-- from
--   spectrum_owid_global_cases_raw so
--   join spectrum_country_mapping scm on so.country = scm.owid_country
-- where country <> 'Switzerland'
-- union all
-- select  -- Number of deaths for Switzerland -> OWID
--   'Europe' as region,
--   'Switzerland' as country,
--   null as division,
--   so.date,
--   null as new_cases,
--   coalesce(so.new_deaths, 0) as new_deaths
-- from spectrum_owid_global_cases_raw so
-- where country = 'Switzerland'
-- union all
-- select  -- Number of cases for Switzerland -> BAG meldeformular, by division
--   'Europe' as region,
--   'Switzerland' as country,
--   sc.gisaid_division as division,
--   bdm.fall_dt as date,
--   count(*) as new_cases,
--   null as new_deaths
-- from
--   bag_dashboard_meldeformular bdm
--   join swiss_canton sc on bdm.ktn = sc.canton_code
-- group by sc.gisaid_division, bdm.fall_dt;


grant select
on table
  spectrum_account,
  spectrum_country,
  spectrum_waste_water_result,
  spectrum_waste_water_variant,
  gisaid_sequence,
  gisaid_api_sequence,
  consensus_sequence,
  automation_state,
  rxiv_article,
  rxiv_author,
  rxiv_article__rxiv_author,
  pangolin_lineage_alias,
  pangolin_lineage__rxiv_article,
  gene,
  ext_owid_global_cases,
  spectrum_cases
to spectrum;

grant select, insert, update
on table
  spectrum_pangolin_lineage_recent_metrics,
  spectrum_new_interesting_variant,
  spectrum_api_usage_sample,
  spectrum_api_cache_sample
to spectrum;

grant select, insert, update, delete
on table
  spectrum_collection,
  spectrum_collection_variant
to spectrum;


grant usage, select
on sequence
  spectrum_pangolin_lineage_recent_metrics_id_seq,
  spectrum_new_interesting_variant_id_seq,
  spectrum_api_usage_sample_id_seq,
  spectrum_api_cache_sample_id_seq
to spectrum;


grant select, insert, update, delete
on table
  spectrum_waste_water_result,
  spectrum_waste_water_variant
to group_cbg;


grant insert
on table
  spectrum_usage_browser,
  spectrum_usage_geo,
  spectrum_usage_hour,
  spectrum_usage_os,
  spectrum_usage_referrer
to spectrum_log_writer;

grant usage, select
on sequence
  spectrum_usage_browser_id_seq,
  spectrum_usage_geo_id_seq,
  spectrum_usage_hour_id_seq,
  spectrum_usage_os_id_seq,
  spectrum_usage_referrer_id_seq
to spectrum_log_writer;
