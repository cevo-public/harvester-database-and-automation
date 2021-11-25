source("R/automation/automation_base.R")
source("R/import_sequencing_qa.R")
source("R/import_sequence_diagnostic.R")
source("R/import_sequence_qc.R")
# source("R/import_frameshift_deletion_diagnostic.R")
source("R/import_ethid.R")
source("R/import_fgcz_failed_seqs.R")

program <- function () {
  db_connection <- open_database_connection()

  # Execute python/import_sequences.py
  # print("Executing import_sequences.py")
  # exit_code <- system(paste0(
  #   "python3 python/import_sequences.py --automated"
  # ))
  # if (exit_code != 0) {
  #   stop("import_sequences.py encoutered an error!")
  # }

  # Exectute import failed sequences from FGCZ
  print("Executing import_fgcz_failed_seqs.R")
  import_fgcz_failed_seqs("/mnt/backup/sampleset", db_connection)

  # Execute import_ethid.R
  print("Executing import_ethid.R")
  import_ethid(db_connection)

  # Execute import_sequencing_qa.R
  print("Executing import_sequencing_qa.R")
  import_sequencing_qa(db_connection, "/mnt/backup/working/qa.csv", FALSE)

  # Execute import_sequence_diagnostic.R to run Nextstrain diagnostic script
  print("Executing import_sequence_diagnostic.R")
  import_sequence_diagnostic(db_connection)

  print("Executing import_sequence_qc.R")
  import_sequence_qc(db_connection)

  # print("Executing import_frameshift_deletion_diagnostic.R")
  # import_deletion_diagnostics("/mnt/backup/working/samples", db_connection)

  # TODO Collect the results (how many sequences were inserted etc.) of the three actions and send a notification email.

  DBI::dbDisconnect(db_connection)
}

automation_start(program, "import_sequences")
