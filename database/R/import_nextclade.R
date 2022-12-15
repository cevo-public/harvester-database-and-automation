#' This function uses the results (in the form of a csv file) of a Nextclade analysis and inserts the found
#' amino acid mutations into the database.
#'
#' Nextclade provides both a web interface and a CLI:
#'   * https://clades.nextstrain.org/
#'   * https://github.com/nextstrain/nextclade
#' return The imported data
import_nextclade_mutations_aa <- function (db_connection, nextclade_file) {
  nextclade_mutations_aa <- read_delim(nextclade_file, delim = ";") %>%
    mutate(
      aaSubstitutions = replace_na(aaSubstitutions, ""),
      aaDeletions = replace_na(aaDeletions, ""),
      mutations = paste(aaSubstitutions, aaDeletions, sep = ",")
    ) %>%
    select(seqName, mutations) %>%
    separate_rows(mutations, sep = ",") %>%
    rename(mutation = mutations) %>%
    mutate(mutation = na_if(str_trim(mutation), "")) %>%
    drop_na(mutation) %>%
    rename(
      sample_name = seqName,
      aa_mutation = mutation
    )

  DBI::dbBegin(db_connection)
  DBI::dbAppendTable(db_connection, name = "consensus_sequence_mutation_aa", nextclade_mutations_aa)
  DBI::dbCommit(db_connection)

  return(nextclade_mutations_aa)
}


#' This function uses the results (in the form of a csv file) of a Nextclade analysis and inserts everything but the
#' found amino acid mutations into the database.
#'
#' Nextclade provides both a web interface and a CLI:
#'   * https://clades.nextstrain.org/
#'   * https://github.com/nextstrain/nextclade
#' return The imported data
import_nextclade_data_without_mutations <- function (db_connection, nextclade_file) {
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
      sample_name = seqName,
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
      sample_name, nextclade_clade, nextclade_qc_overall_score, nextclade_qc_overall_status, nextclade_total_gaps,
      nextclade_total_insertions, nextclade_total_missing, nextclade_total_mutations, nextclade_total_non_acgtns,
      nextclade_total_pcr_primer_changes, nextclade_alignment_start, nextclade_alignment_end,
      nextclade_alignment_score, nextclade_qc_missing_data_score, nextclade_qc_missing_data_status,
      nextclade_qc_missing_data_total, nextclade_qc_mixed_sites_score, nextclade_qc_mixed_sites_status,
      nextclade_qc_mixed_sites_total, nextclade_qc_private_mutations_cutoff, nextclade_qc_private_mutations_excess,
      nextclade_qc_private_mutations_score, nextclade_qc_private_mutations_status,
      nextclade_qc_private_mutations_total, nextclade_qc_snp_clusters_clustered, nextclade_qc_snp_clusters_score,
      nextclade_qc_snp_clusters_status, nextclade_qc_snp_clusters_total, nextclade_errors
    )

  table_spec <- parse_table_specification(
    table_name = "consensus_sequence_meta", db_connection = db_connection)
  update_table(
    "consensus_sequence_meta",
    nextclade_data,
    db_connection,
    append_new_rows = FALSE,
    cols_to_update = c(
      "nextclade_clade",
      "nextclade_qc_overall_score",
      "nextclade_qc_overall_status",
      "nextclade_total_gaps",
      "nextclade_total_insertions",
      "nextclade_total_missing",
      "nextclade_total_mutations",
      "nextclade_total_non_acgtns",
      "nextclade_total_pcr_primer_changes",
      "nextclade_alignment_start",
      "nextclade_alignment_end",
      "nextclade_alignment_score",
      "nextclade_qc_missing_data_score",
      "nextclade_qc_missing_data_status",
      "nextclade_qc_missing_data_total",
      "nextclade_qc_mixed_sites_score",
      "nextclade_qc_mixed_sites_status",
      "nextclade_qc_mixed_sites_total",
      "nextclade_qc_private_mutations_cutoff",
      "nextclade_qc_private_mutations_excess",
      "nextclade_qc_private_mutations_score",
      "nextclade_qc_private_mutations_status",
      "nextclade_qc_private_mutations_total",
      "nextclade_qc_snp_clusters_clustered",
      "nextclade_qc_snp_clusters_score",
      "nextclade_qc_snp_clusters_status",
      "nextclade_qc_snp_clusters_total",
      "nextclade_errors"
    ),
    key_col = "sample_name",
    table_spec = table_spec,
    close_con = FALSE
  )
  DBI::dbCommit(db_connection)
  return(nextclade_data)
}


import_gisaid_nextclade_mutations_aa <- function (db_connection, nextclade_file, use_transaction = TRUE,
                                                  staging = FALSE) {
  nextclade_mutations_aa <- read_delim(nextclade_file, delim = ";", col_types = cols(
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
    mutate(
      aaSubstitutions = replace_na(aaSubstitutions, ""),
      aaDeletions = replace_na(aaDeletions, ""),
      mutations = paste(aaSubstitutions, aaDeletions, sep = ",")
    ) %>%
    select(seqName, mutations) %>%
    separate_rows(mutations, sep = ",") %>%
    rename(mutation = mutations) %>%
    mutate(mutation = na_if(str_trim(mutation), "")) %>%
    drop_na(mutation) %>%
    rename(
      strain = seqName,
      aa_mutation = mutation
    )

  if (use_transaction) DBI::dbBegin(db_connection)
  tbl_name <- "gisaid_sequence_nextclade_mutation_aa"
  if (staging) tbl_name <- "gisaid_sequence_nextclade_mutation_aa_staging"
  DBI::dbAppendTable(db_connection, name = tbl_name, nextclade_mutations_aa)
  if (use_transaction) DBI::dbCommit(db_connection)

  return(nextclade_mutations_aa)
}


get_sample_names_without_nextclade <- function (db_connection) {
  sql <- "
    select csm.sample_name
    from consensus_sequence_meta csm
    where csm.nextclade_qc_overall_score is null and csm.nextclade_errors != '\"Unable to align: no seed matches\"'
    order by random()
    limit 5000;
  "
  res <- DBI::dbSendQuery(conn = db_connection, statement = sql)
  data <- DBI::dbFetch(res)
  DBI::dbClearResult(res)
  return(data$sample_name)
}
