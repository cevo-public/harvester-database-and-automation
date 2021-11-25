-- Should probably be transferred to init.sql once it is finalized.
drop table pubmed_article__pubmed_author, pubmed_author, pubmed_article;
truncate pubmed_article__pubmed_author, pubmed_author, pubmed_article;
create table pubmed_article (
  pmid bigint primary key,
  date_completed date,
  date_revised date,
  article_title text not null,
  journal_title text,
  abstract text,
  language text
);

create table pubmed_author (
  id bigserial primary key,
  lastname text,
  forename text,
  collective_name text
);

create table pubmed_article__pubmed_author (
  pmid bigint references pubmed_article (pmid) on update cascade on delete cascade,
  author_id bigint references pubmed_author (id) on update cascade on delete cascade,
  primary key (pmid, author_id)
);



-- A very slow implementation with O(N*M), has the problem that for "B.1.1.7", it also returns "B.1.1", etc.
-- create materialized view pangolin_lineage__pubmed_article as
-- with lineages as (
--   select distinct pangolin_lineage
--   from spectrum_sequence_public_meta
--   where char_length(pangolin_lineage) >= 3 -- We don't want single letter lineages such as "B"
-- )
-- select l.pangolin_lineage, par.pmid
-- from
--   lineages l
--   join pubmed_article par on (par.abstract || par.article_title) like ('%' || l.pangolin_lineage || '%');

-- Better: Using a regex to find potential pangolin lineages
drop materialized view pangolin_lineage__pubmed_article;
create materialized view pangolin_lineage__pubmed_article as
with lineages as (
  select distinct pangolin_lineage
  from gisaid_sequence
),
article_potential_lineage as (
  select distinct
    par.pmid,
    (regexp_matches(par.article_title || ' ' || par.abstract, '(([VMDWBSNLZGPUKAYRC])(\.[0-9]+)+)', 'g'))[1]
      as maybe_pangolin_linage
  from pubmed_article par
)
select l.pangolin_lineage, a.pmid
from
  lineages l
  join article_potential_lineage a on l.pangolin_lineage = a.maybe_pangolin_linage;


create table rxiv_article (
  doi text primary key,
  version integer,
  title text,
  date date,
  type text,
  category text,
  abstract text,
  license text,
  server text,
  jatsxml text,
  published text
);

create table rxiv_author (
  id serial primary key,
  name text
);

create table rxiv_article__rxiv_author (
  doi text references rxiv_article (doi) on update cascade on delete cascade,
  author_id integer references rxiv_author (id) on update cascade on delete cascade,
  position integer,
  primary key (doi, author_id, position)
);


refresh materialized view concurrently pangolin_lineage__rxiv_article;
create materialized view pangolin_lineage__rxiv_article as
with lineages as (
  select distinct pangolin_lineage
  from gisaid_api_sequence
),
article_potential_lineage as (
  select distinct
    rar.doi,
    (regexp_matches(rar.title || ' ' || rar.abstract, '(([VMDWBSNLZGPUKAYRC])(\.[0-9]+)+)', 'g'))[1]
      as maybe_pangolin_linage
  from rxiv_article rar
)
select l.pangolin_lineage, a.doi
from
  lineages l
  join article_potential_lineage a on l.pangolin_lineage = a.maybe_pangolin_linage;

create unique index on pangolin_lineage__rxiv_article (pangolin_lineage, doi);
create index on pangolin_lineage__rxiv_article (pangolin_lineage);
