-- Frequencies of mutations
select aa_mutation, count(*)
from gisaid_sequence_nextclade_mutation_aa
group by aa_mutation
order by count(*) desc;


-- Frequencies of mutations per gene
select split_part(aa_mutation, ':', 1) as gene, count(*)
from gisaid_sequence_nextclade_mutation_aa
group by split_part(aa_mutation, ':', 1)
order by count(*) desc;


-- Get the common nucleotide mutations of a pangolin lineage
with sequence as (
  select strain
  from gisaid_sequence s
where
  pangolin_lineage = 'B.1.617.1'
--   and country = 'United Kingdom'
  and exists(select from gisaid_sequence_mutation_nucleotide m where s.strain = m.strain)
)
select
  m.position + 1 as position,
  m.mutation,
  count(*) as count,
  count(*) * 1.0 / (select count(*) from sequence) as proportion
from
  sequence s
  join gisaid_sequence_mutation_nucleotide m on s.strain = m.strain
group by m.position, m.mutation
having count(*) * 1.0 / (select count(*) from sequence) >= 0.2
order by count(*) desc, position;
