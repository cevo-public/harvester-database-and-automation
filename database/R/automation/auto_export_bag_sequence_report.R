source("R/automation/automation_base.R")


program <- function () {
  # Only export the report between 4:00 and 5:00
  if (hour(now(tz = "Europe/Zurich")) < 4 || hour(now(tz = "Europe/Zurich")) > 5) {
    print_log("It's not right right time - do nothing.")
    return()
  }
  db_connection <- open_database_connection()
  bag_sequence_report <- dplyr::tbl(db_connection, "bag_sequence_report") %>%
    collect()
  DBI::dbDisconnect(db_connection)
  write_csv(
    x = bag_sequence_report,
    path = paste(Sys.getenv("BAG_METADATA_DIR"),
                 "sequence_report/bag_sequence_report.csv", sep = "/"))
  send_email(
    "BAG sequence report exported",
    "I exported the BAG sequence report. It should be on Polybox in not more than 10 minutes."
  )
}

automation_start(program, "export_bag_sequence_report")
