-- Update materialized views

refresh materialized view concurrently spectrum_sequence_public_meta;
refresh materialized view concurrently spectrum_sequence_private_meta;
refresh materialized view concurrently spectrum_sequence_public_mutation_aa;
refresh materialized view concurrently spectrum_sequence_intensity;
refresh materialized view concurrently spectrum_pangolin_lineage_mutation;
refresh materialized view concurrently pangolin_lineage__rxiv_article;

truncate spectrum_api_cache_sample;


-- Materialized views can only be refreshed by the owner. However, we would like to use another
-- (technical) user to refresh them.
-- See: https://dba.stackexchange.com/questions/171932/postgresql-9-3-13-how-do-i-refresh-materialised-views-with-different-users

create user mv_refresher password '<password>';

create or replace function refresh_all_mv() returns void security definer as $$
begin
  refresh materialized view concurrently spectrum_sequence_public_meta;
  refresh materialized view concurrently spectrum_sequence_private_meta;
  refresh materialized view concurrently spectrum_sequence_public_mutation_aa;
  refresh materialized view concurrently spectrum_sequence_public_mutation_nucleotide;
  refresh materialized view concurrently spectrum_sequence_intensity;
  refresh materialized view concurrently spectrum_pangolin_lineage_mutation;
  refresh materialized view concurrently spectrum_pangolin_lineage_mutation_nucleotide;
  refresh materialized view concurrently spectrum_swiss_cases;
  refresh materialized view concurrently pangolin_lineage__rxiv_article;
  truncate spectrum_api_cache_sample;
  update automation_state set state = clock_timestamp()::text where program_name = 'refresh_all_mv()';
  return;
end;
$$ language plpgsql;
revoke all on function refresh_all_mv() from public;
grant execute on function refresh_all_mv() to mv_refresher, gisaid_importer;
