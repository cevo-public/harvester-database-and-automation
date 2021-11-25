source("R/automation/automation_base.R")
source('R/import_owid_global_cases.R')


program <- function () {
  db_connection <- open_database_connection()
  import_global_incidence(db_connection)
  import_global_incidence_cov_spectrum(db_connection)
  DBI::dbDisconnect(db_connection)
}

automation_start(program, "import_owid_global_cases")
