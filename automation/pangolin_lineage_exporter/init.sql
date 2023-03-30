DO $$ BEGIN
  PERFORM 'eoc'::regtype;
EXCEPTION
  WHEN undefined_object THEN
    CREATE TYPE eoc AS (ethid INTEGER, sample_number TEXT, order_date DATE, pango_lineage TEXT);
END $$;

DO $$ BEGIN
  PERFORM 'imv'::regtype;
EXCEPTION
  WHEN undefined_object THEN
    CREATE TYPE imv AS (ethid INTEGER, sample_number TEXT, order_date DATE, pango_lineage TEXT, sample_name_anonymised TEXT);
END $$;

CREATE TABLE IF NOT EXISTS pangolin_lineage_exporter_chunk_log (
  chunk INTEGER,
  ethid INTEGER
);

CREATE OR REPLACE FUNCTION eoc_pangolin_lineage() RETURNS SETOF eoc AS $$
  SELECT
    consensus_sequence.ethid,
    eoc_metadata.sample_number,
    test_metadata.order_date,
    consensus_sequence_meta.pango_lineage
  FROM eoc_metadata
  JOIN consensus_sequence ON eoc_metadata.ethid = consensus_sequence.ethid
  JOIN consensus_sequence_meta ON consensus_sequence.sample_name = consensus_sequence_meta.sample_name
  JOIN test_metadata ON eoc_metadata.ethid = test_metadata.ethid
  LEFT JOIN pangolin_lineage_exporter_chunk_log ON test_metadata.ethid = pangolin_lineage_exporter_chunk_log.ethid
  WHERE pangolin_lineage_exporter_chunk_log.ethid IS NULL
    AND consensus_sequence_meta.pango_lineage IS NOT NULL
  ORDER BY consensus_sequence.ethid, test_metadata.order_date
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION imv_pangolin_lineage() RETURNS SETOF imv AS $$
  SELECT
    consensus_sequence.ethid,
    imv_metadata.sample_number,
    test_metadata.order_date,
    consensus_sequence_meta.pango_lineage,
    imv_metadata.sample_name_anonymised
  FROM imv_metadata
  JOIN consensus_sequence ON imv_metadata.ethid = consensus_sequence.ethid
  JOIN consensus_sequence_meta ON consensus_sequence.sample_name = consensus_sequence_meta.sample_name
  JOIN test_metadata ON imv_metadata.ethid = test_metadata.ethid
  LEFT JOIN pangolin_lineage_exporter_chunk_log ON test_metadata.ethid = pangolin_lineage_exporter_chunk_log.ethid
  WHERE pangolin_lineage_exporter_chunk_log.ethid IS NULL
    AND consensus_sequence_meta.pango_lineage IS NOT NULL
  ORDER BY consensus_sequence.ethid, test_metadata.order_date
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION pangolin_lineage(
  lab ANYELEMENT,
  time_limit_in_days SMALLINT,
  number_limit INTEGER
) RETURNS SETOF ANYELEMENT AS $$
BEGIN
  RETURN QUERY EXECUTE
    'SELECT * FROM ' || pg_typeof(lab) || '_pangolin_lineage()
      WHERE (SELECT count(*) FROM ' || pg_typeof(lab) || '_pangolin_lineage()) >= $2
        OR (SELECT min(order_date) FROM ' || pg_typeof(lab) || E'_pangolin_lineage()) < now() - $1
      LIMIT $2'
  USING (time_limit_in_days || ' DAYS')::INTERVAL, number_limit;
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION pangolin_lineage(
  lab ANYELEMENT,
  last_chunk INTEGER,
  time_limit_in_days SMALLINT,
  number_limit INTEGER
) RETURNS SETOF ANYELEMENT AS $$
DECLARE
  last_chunk_logged INTEGER;
  row ALIAS FOR $0;
BEGIN
  EXECUTE 'SELECT COALESCE(MAX(chunk), 0) FROM pangolin_lineage_exporter_chunk_log
    JOIN ' || pg_typeof(lab) || '_metadata lab ON pangolin_lineage_exporter_chunk_log.ethid = lab.ethid'
    INTO last_chunk_logged;
  IF last_chunk_logged != last_chunk THEN
    RAISE EXCEPTION 'The last logged chunk is % not %.', last_chunk_logged, last_chunk;
  END IF;

  FOR row IN SELECT * FROM pangolin_lineage(lab, time_limit_in_days, number_limit)
  LOOP
    INSERT INTO pangolin_lineage_exporter_chunk_log VALUES (last_chunk + 1, row.ethid);
    RETURN NEXT row;
  END LOOP;
  RETURN;
END
$$ LANGUAGE plpgsql;
