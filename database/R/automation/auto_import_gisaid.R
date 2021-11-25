# This script uses a lot of absolute paths and is designed to work with the gisaid_importer Docker image defined in
# the automation repository.

source("R/automation/automation_base.R")
source("R/import_nextclade.R")


metadata_path <- "/data/metadata.tsv"
sequences_path <- "/data/sequences.fasta"


program <- function () {
  chunk_size <- 10000

  # ----- Create temporary working directory
  print_log("Create temporary working directory")
  system("rm -rf /app_wkdir")
  system("mkdir -p /app_wkdir")
  setwd("/app_wkdir")

  # ----- Split fasta file into chunks
  print_log("Split fasta file into chunks")
  system("mkdir -p splits")
  setwd("splits")
  exit_code <- system(paste0("
  awk -v size=", chunk_size, " '
     /^>/ { n++; if (n % size == 1) { close(fname); fname = sprintf(\"%d.fasta\", n); print fname } }
     { print >> fname }
  ' ", sequences_path))
  if (exit_code != 0) {
    stop(paste0("Fasta file splitting failed with exit code ", exit_code))
  }
  setwd("..")

  # ----- Align the small fasta files (parallel process)
  print_log("Align the small fasta files")
  exit_code <- system("bash /app/align_splits.sh -r /app/reference.fasta")
  if (exit_code != 0) {
    stop(paste0("Alignment failed with exit code ", exit_code))
  }

  # ----- Run Nextclade on the small fasta files (parallel process)
  exit_code <- system("bash /app/nextclade_splits.sh")
  if (exit_code != 0) {
    stop(paste0("Nextclade failed with exit code ", exit_code))
  }

  # ----- Insert the splits
  print_log("Inserting data")
  setwd("/app")
  exit_code <- system("bash /app/insert_data.sh")
  if (exit_code != 0) {
    stop(paste0("Insert data failed with exit code ", exit_code))
  }

  # ----- Switch in the _staging tables and truncate the old ones
  print_log(" Switch in the _staging tables and truncate the old ones")
  db_connection <- open_database_connection()
  DBI::dbExecute(db_connection, "alter table gisaid_sequence rename to gisaid_sequence_old;")
  DBI::dbExecute(db_connection, "alter table gisaid_sequence_nextclade_mutation_aa rename to gisaid_sequence_nextclade_mutation_aa_old;")
  DBI::dbExecute(db_connection, "alter table gisaid_sequence_staging rename to gisaid_sequence;")
  DBI::dbExecute(db_connection, "alter table gisaid_sequence_nextclade_mutation_aa_staging rename to gisaid_sequence_nextclade_mutation_aa;")
  DBI::dbExecute(db_connection, "vacuum analyse gisaid_sequence, gisaid_sequence_nextclade_mutation_aa;")
  DBI::dbExecute(db_connection, "truncate gisaid_sequence_nextclade_mutation_aa_old, gisaid_sequence_old;")
  DBI::dbExecute(db_connection, "vacuum full gisaid_sequence_nextclade_mutation_aa_old, gisaid_sequence_old;")
  DBI::dbExecute(db_connection, "alter table gisaid_sequence_old rename to gisaid_sequence_staging;")
  DBI::dbExecute(db_connection, "alter table gisaid_sequence_nextclade_mutation_aa_old rename to gisaid_sequence_nextclade_mutation_aa_staging;")
  DBI::dbExecute(db_connection, "update gisaid_sequence set date = null where date = '2020-01-01';")
  DBI::dbExecute(db_connection, "update gisaid_sequence gs set iso_country = gc.iso_country from gisaid_country gc where gs.country = gc.gisaid_country;")
  DBI::dbExecute(db_connection, "update gisaid_sequence gs set iso_country_exposure = gc.iso_country from gisaid_country gc where gs.country_exposure = gc.gisaid_country;")
  # Fix our submitting_lab. This is hopefully only a temporary step that can be removed soon.
  DBI::dbExecute(db_connection, "update gisaid_sequence set submitting_lab = 'Department of Biosystems Science and Engineering, ETH Zürich' where gisaid_sequence.submitting_lab = 'Department of Biosystems Science and Engineering, ETH Zurich';")
  DBI::dbDisconnect(db_connection)

  # ----- Send email notification
  # send_email(
  #   "New GISAID data imported",
  #   "I imported new GISAID data!")

  # ----- Clean up
  system("rm -rf /app_wkdir")
  print_log("Finished!")
}


program_import_split <- function (spl) {
  print_log(paste0("Start importing split ", spl))
  setwd("/app_wkdir")

  # Prepare metadata
  metadata <- read_tsv(metadata_path) %>%
    rename(
      nextstrain_clade = Nextstrain_clade,
      gisaid_clade = GISAID_clade,
      pangolin_lineage = pango_lineage
    ) %>%
    mutate(
      date_str = date,
      length = as.integer(length)
    ) %>%
    select(
      strain, virus, gisaid_epi_isl, genbank_accession, date, date_str, region, country, division, location,
      region_exposure, country_exposure, division_exposure, segment, length, host, age, sex, nextstrain_clade,
      pangolin_lineage, gisaid_clade, originating_lab, submitting_lab, authors, url, title, paper_url, date_submitted,
      purpose_of_sequencing
    )

  # Handle unprecise dates (yyyy, yyyy-mm, yyyy-mm-XX)
  mds <- metadata$date
  mds[str_sub(mds, 9, 10) == 'XX'] <- paste0(str_sub(mds[str_sub(mds, 9, 10) == 'XX'], 1, 7), '-01')
  mds[str_length(mds) == 7] <- paste0(mds[str_length(mds) == 7], '-01')
  mds[str_length(mds) == 4] <- paste0(mds[str_length(mds) == 4], '-01-01')
  metadata$date <- ymd(mds)

  # Replace ? with NA in age
  metadata <- metadata %>%
    mutate(age = as.integer(na_if(age, "?")))

  # Writing to the database
  db_connection <- open_database_connection()

  # Sequence strings
  original_fasta <- Biostrings::readDNAStringSet(paste0("splits/", spl, ".fasta"))
  original_seqs <- tibble(strain = names(original_fasta), original_seq = paste(original_fasta))
  aligned_fasta <- Biostrings::readDNAStringSet(paste0("alignments/", spl, ".fasta"))
  aligned_seqs <- tibble(strain = names(aligned_fasta), aligned_seq = paste(aligned_fasta))

  # Nextclade without mutations
  nextclade_file <- paste0("nextclade/", spl, ".csv")
  nextclade_data <- read_delim(nextclade_file, delim = ";", col_types = cols(
    .default = col_double(),
    seqName = col_character(),
    clade = col_character(),
    qc.overallStatus = col_character(),
    substitutions = col_character(),
    deletions = col_character(),
    insertions = col_character(),
    missing = col_character(),
    nonACGTNs = col_character(),
    pcrPrimerChanges = col_character(),
    aaSubstitutions = col_character(),
    aaDeletions = col_character(),
    qc.missingData.status = col_character(),
    qc.mixedSites.status = col_character(),
    qc.privateMutations.status = col_character(),
    qc.snpClusters.clusteredSNPs = col_character(),
    qc.snpClusters.status = col_character(),
    errors = col_character()
  )) %>%
    rename(
      strain = seqName,
      nextclade_clade = clade,
      nextclade_qc_overall_score = qc.overallScore,
      nextclade_qc_overall_status = qc.overallStatus,
      nextclade_total_gaps = totalGaps,
      nextclade_total_insertions = totalInsertions,
      nextclade_total_missing = totalMissing,
      nextclade_total_mutations = totalMutations,
      nextclade_total_non_acgtns = totalNonACGTNs,
      nextclade_total_pcr_primer_changes = totalPcrPrimerChanges,
      nextclade_alignment_start = alignmentStart,
      nextclade_alignment_end = alignmentEnd,
      nextclade_alignment_score = alignmentScore,
      nextclade_qc_missing_data_score = qc.missingData.score,
      nextclade_qc_missing_data_status = qc.missingData.status,
      nextclade_qc_missing_data_total = qc.missingData.totalMissing,
      nextclade_qc_mixed_sites_score = qc.mixedSites.score,
      nextclade_qc_mixed_sites_status = qc.mixedSites.status,
      nextclade_qc_mixed_sites_total = qc.mixedSites.totalMixedSites,
      nextclade_qc_private_mutations_cutoff = qc.privateMutations.cutoff,
      nextclade_qc_private_mutations_excess = qc.privateMutations.excess,
      nextclade_qc_private_mutations_score = qc.privateMutations.score,
      nextclade_qc_private_mutations_status = qc.privateMutations.status,
      nextclade_qc_private_mutations_total = qc.privateMutations.total,
      nextclade_qc_snp_clusters_clustered = qc.snpClusters.clusteredSNPs,
      nextclade_qc_snp_clusters_score = qc.snpClusters.score,
      nextclade_qc_snp_clusters_status = qc.snpClusters.status,
      nextclade_qc_snp_clusters_total = qc.snpClusters.totalSNPs,
      nextclade_errors = errors
    ) %>%
    select(
      strain, nextclade_clade, nextclade_qc_overall_score, nextclade_qc_overall_status, nextclade_total_gaps,
      nextclade_total_insertions, nextclade_total_missing, nextclade_total_mutations, nextclade_total_non_acgtns,
      nextclade_total_pcr_primer_changes, nextclade_alignment_start, nextclade_alignment_end,
      nextclade_alignment_score, nextclade_qc_missing_data_score, nextclade_qc_missing_data_status,
      nextclade_qc_missing_data_total, nextclade_qc_mixed_sites_score, nextclade_qc_mixed_sites_status,
      nextclade_qc_mixed_sites_total, nextclade_qc_private_mutations_cutoff, nextclade_qc_private_mutations_excess,
      nextclade_qc_private_mutations_score, nextclade_qc_private_mutations_status,
      nextclade_qc_private_mutations_total, nextclade_qc_snp_clusters_clustered, nextclade_qc_snp_clusters_score,
      nextclade_qc_snp_clusters_status, nextclade_qc_snp_clusters_total, nextclade_errors
    )

  data <- original_seqs %>%
    inner_join(aligned_seqs, by = "strain") %>%
    inner_join(metadata, by = "strain") %>%
    left_join(nextclade_data, by = "strain")
  DBI::dbAppendTable(db_connection, name = "gisaid_sequence_staging", data)

  # Nextclade mutations
  import_gisaid_nextclade_mutations_aa(db_connection, nextclade_file, use_transaction = FALSE, staging = TRUE)

  DBI::dbDisconnect(db_connection)

  print_log(paste0("Finished importing split ", spl))
}


args <- commandArgs(trailingOnly=TRUE)

if (length(args) == 0) {
  stop("Missing command line arguments: no mode specified.")
} else if (args[1] == "--main") {
  program()
} else if (args[1] == "--import-split") {
  program_import_split(args[2])
} else {
  stop("Unknown mode.")
}
