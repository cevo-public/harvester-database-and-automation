# The script expects `nextclade` to be installed and in the PATH.

source("R/automation/automation_base.R")
source('R/import_nextclade.R')


excluded_sample_names_aa <- NULL

program <- function () {
  db_connection <- open_database_connection()

  # Find samples that do not have any mutations assigned
  sample_names_aa <- get_sample_names_without_nextclade(db_connection)

  # Subtract those in excluded_sample_names
  sample_names_aa <- sample_names_aa[! sample_names_aa %in% excluded_sample_names_aa]

  if (length(sample_names_aa) == 0) {
    print("Nothing to do.")
    DBI::dbDisconnect(db_connection)
    return()
  }

  # Write the sequences into a fasta file
  fasta_file_aa <- "tmp_samples_for_nextclade_aa.fasta"
  export_seqs_as_fasta(db_connection, sample_names_aa, fasta_file_aa, overwrite = TRUE)

  # Call nextclade
  nextclade_file_aa <- "tmp_nextclade_aa.csv"
  exit_code <- system(paste0(
    "nextclade --input-fasta ", fasta_file_aa, " --output-csv ", nextclade_file_aa
  ))
  if (exit_code != 0) {
    stop("Nextclade execution failed")
  }

  # Import the mutations
  imported_data_aa <- import_nextclade_mutations_aa(db_connection, nextclade_file_aa)

  # Import everything else
  imported_data <- import_nextclade_data_without_mutations(db_connection, nextclade_file_aa)

  # For some sequences, Nextclade don't find any mutations. Either because they really are exactly the same as the
  # reference genome or (more likely) their quality is not good enough for a Nextclade analysis. To avoid them being
  # processed every time, they will be stored in excluded_sample_names.
  imported_sample_names_aa <- unique(imported_data_aa$sample_name)
  excluded_sample_names_aa <<- c(
    excluded_sample_names_aa,
    sample_names_aa[! sample_names_aa %in% imported_sample_names_aa]
  )

  # Send email notification
  send_email("New Nextclade mutations imported", paste0(
    "Nextclade analyzed ", length(sample_names_aa), " sequences and found amino acid mutations for ",
    length(imported_sample_names_aa), " sequences. They were added into the database."
  ))

  DBI::dbDisconnect(db_connection)
}

automation_start(program, "import_nextclade_mutations")
