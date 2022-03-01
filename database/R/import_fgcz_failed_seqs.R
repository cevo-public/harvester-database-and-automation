#' Update table "consensus_sequence" with samples that fail entirely (sample_name, all N sequence) from FGCZ. These samples are not returned normally and instead are listed in files by Ivan.
#' @param sampleset_dir Path to V-pipe sampleset directory on DBSSE, which contain the missing data txt files
#' e.g. "/Volumes/covid19-pangolin/backup/sampleset"
import_fgcz_failed_seqs <- function(
  sampleset_dir, db_connection
) {
  files_to_import <- list.files(path = sampleset_dir, pattern = "^missing.*.txt$", full.names = F, recursive = F)
  n_files_to_import <- length(files_to_import)
  print(paste("Found files containing failed samples from", n_files_to_import, "batches."))
  for (file_i in files_to_import) {
    sequencing_batch <- strsplit(x = file_i, split = "\\.")[[1]][2]
    sample_names <- read.delim(file = paste(sampleset_dir, file_i, sep = "/"), header = F)
    sample_names[, 1] <- as.character(sample_names[, 1])

    failed_seq_data <- data.frame(
      sample_name = sample_names$V1,
      sequencing_batch = sequencing_batch,
      seq = paste0(rep("N", 29903), collapse = ""),
      sequencing_center = "fgcz"
    )

    # Skip if any failed seqs already in database - maybe they were re-run in another batch and succeeded?
    db_sample_data <- dplyr::tbl(db_connection, "consensus_sequence") %>%
      filter(sample_name %in% !! sample_names$V1) %>%
      select(sample_name, number_n) %>%
      collect()
    if (nrow(db_sample_data) > 0) {
      print(paste("Not importing", nrow(db_sample_data), "out of", nrow(failed_seq_data),
                  "failed samples because they're already in consensus_sequence."))
      failed_seq_data <- failed_seq_data %>%
        filter(!(sample_name %in% db_sample_data$sample_name))
    }

    table_spec <- parse_table_specification(
      table_name = "consensus_sequence",
      db_connection = db_connection)
    update_table(
      table_name = "consensus_sequence",
      new_table = failed_seq_data,
      con = db_connection,
      cols_to_update = c("seq", "sequencing_batch", "sequencing_center"),
      key_col = "sample_name",
      table_spec = table_spec
    )
    print(paste("Finished importing FGCZ failed samples for batch", sequencing_batch))
  }
}
