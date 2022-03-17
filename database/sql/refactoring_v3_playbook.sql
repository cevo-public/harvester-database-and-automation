-- (1)
-- Delete old tables.

drop table
  z_consensus_sequence,
  z_consensus_sequence_meta,
  z_consensus_sequence_mutation_aa,
  z_consensus_sequence_mutation_nucleotide,
  z_consensus_sequence_notes,
  z_extraction_plate,
  z_sequencing_plate,
  z_test_metadata,
  z_test_plate_mapping,
  test_metadata,
  test_plate_mapping,
  extraction_plate,
  sequencing_plate;

drop table sequencing_plate;

-- (2)
-- Execute refactoring_v3_schema.sql

-- (3)
-- Execute refactoring_v3_migration.sql

-- (4)
-- Backup old tables

alter table viollier_plate rename to backup_220317_viollier_plate;
alter table viollier_test rename to backup_220317_viollier_test;
alter table viollier_test__viollier_plate rename to backup_220317_viollier_test__viollier_plate;
alter table non_viollier_test rename to backup_220317_non_viollier_test;
alter table consensus_sequence rename to backup_220317_consensus_sequence;
alter table consensus_sequence_meta rename to backup_220317_consensus_sequence_meta;
alter table consensus_sequence_mutation_aa rename to backup_220317_consensus_sequence_mutation_aa;
alter table consensus_sequence_mutation_nucleotide rename to backup_220317_consensus_sequence_mutation_nucleotide;
alter table consensus_sequence_nextclade_data rename to backup_220317_consensus_sequence_nextclade_data;
alter table consensus_sequence_notes rename to backup_220317_consensus_sequence_notes;
alter table consensus_sequence_unaligned_nextclade_data rename to backup_220317_consensus_sequence_unaligned_nextclade_data;

-- (5)
-- Remove z_ prefix

alter table z_consensus_sequence rename to consensus_sequence;
alter table z_consensus_sequence_meta rename to consensus_sequence_meta;
alter table z_consensus_sequence_mutation_aa rename to consensus_sequence_mutation_aa;
alter table z_consensus_sequence_mutation_nucleotide rename to consensus_sequence_mutation_nucleotide;
alter table z_consensus_sequence_notes rename to consensus_sequence_notes;
alter table z_extraction_plate rename to extraction_plate;
alter table z_sequencing_plate rename to sequencing_plate;
alter table z_test_metadata rename to test_metadata;
alter table z_test_plate_mapping rename to test_plate_mapping;
