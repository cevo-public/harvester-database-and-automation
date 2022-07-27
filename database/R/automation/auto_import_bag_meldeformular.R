source("R/automation/automation_base.R")
source('R/import_bag_meldeformular.R')


program <- function () {
  db_connection <- open_database_connection()
  out <- tryCatch({
    import_bag_meldeformular(db_connection, Sys.getenv("BAG_METADATA_DIR"), part_of_automation = TRUE)
  },
  error=function(cond) {
            message(cond)
            return(NA)
        },
  finally={
        DBI::dbDisconnect(db_connection)       
  })
  return(out)    
}

automation_start(program, "import_bag_meldeformular")
