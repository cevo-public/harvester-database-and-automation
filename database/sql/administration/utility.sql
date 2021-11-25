-- Display the disk usage of the tables
-- See https://wiki.postgresql.org/wiki/Disk_Usage
select
  pg_size_pretty(sum(total_bytes))
from
  (
    select
      oid,
      table_name,
      row_estimate,
      pg_size_pretty(total_bytes) as total,
      pg_size_pretty(table_bytes) as table,
      pg_size_pretty(index_bytes) as index,
      pg_size_pretty(toast_bytes) as toast,
      total_bytes,
      table_bytes,
      index_bytes,
      toast_bytes
    from (
      select
        *,
        total_bytes - index_bytes - coalesce(toast_bytes, 0) as table_bytes
      from (
        select
          c.oid,
          relname as table_name,
          c.reltuples as row_estimate,
          pg_total_relation_size(c.oid) as total_bytes,
          pg_indexes_size(c.oid) as index_bytes,
          pg_total_relation_size(reltoastrelid) as toast_bytes
        from pg_class c
        left join pg_namespace n on n.oid = c.relnamespace
        where
          relkind in ('r', 'm')
          and nspname = 'public'
      ) a
    ) a
    order by table_name
  ) x;


-- Show the running transactions
select *
from pg_stat_activity;
