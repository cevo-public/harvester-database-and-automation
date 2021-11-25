source("R/automation/automation_base.R")
source("R/import_bag_data_for_dashboard.R")


program <- function() {
  db_connection <- open_database_connection()

  # Don't sync before 13:30 and after 23:00
  if (now() < ymd_hm(paste(format_ISO8601(today()), "13:30"), tz = "CET")
    || now() > ymd_hm(paste(format_ISO8601(today()), "23:00"), tz = "CET")) {
    return()
  }
  # Don't sync on weekends
  current_weekday <- lubridate::wday(now(), week_start = 1)
  if (current_weekday == 6 || current_weekday == 7) {
    return()
  }
  # Exclude holidays
  ch_holidays <- c(
    ymd("2021-01-01"), ymd("2021-01-02"), ymd("2021-01-06"),
    ymd("2021-03-19"), ymd("2021-04-02"), ymd("2021-04-05"),
    ymd("2021-05-01"), ymd("2021-05-13"), ymd("2021-05-24"),
    ymd("2021-08-01"), ymd("2021-12-25"), ymd("2021-12-26")
  )
  if (today() %in% ch_holidays) {
    return()
  }

  dashboard_bag_data_info <- get_newest_bag_data_for_dashboard_dir(Sys.getenv("BAG_COVID_19_DIR"))
  dashboard_state <- tbl(db_connection, "dashboard_state") %>% collect()
  if (identical(dashboard_state$last_data_update, dashboard_bag_data_info$timestamp)) {
    print(paste0(Sys.time(), ": No new data available. Current data remains ", dashboard_bag_data_info$timestamp))
  } else {
    print(paste0(Sys.time(), ": Found new data: ", dashboard_bag_data_info$timestamp, ". Start updating."))
    import_bag_data_for_dashboard(dashboard_bag_data_info$path, dashboard_bag_data_info$timestamp, db_connection)
    print(paste0(Sys.time(), ": Update finished. Current data: ", dashboard_bag_data_info$timestamp))
    send_email(
      "New daily BAG metadata (for dashboard) imported",
      paste0("I finished importing new BAG metadata from the following file:\n\n",
             dashboard_bag_data_info$path))
  }

  DBI::dbDisconnect(db_connection)
}

automation_start(program, "import_bag_meldeformular_dashboard")
