# This file contains fundamental functions for automation and should be loaded by every automation script.

library(glue)
library(tidyverse)
library(lubridate)

source('R/utility.R')


print_log <- function (text) {
  print(paste0("[", lubridate::now(), "] ", text))
}


automation_start <- function (program_function, program_name) {
  while (TRUE) {
    print_log(paste0("Starting ", program_name))
    program_function()
    print_log(paste0(program_name, " finished"))
    print_log(paste0("Next run is in ", Sys.getenv("CHECK_FOR_NEW_DATA_INTERVAL_SECONDS"),
                 " seconds."))
    Sys.sleep(Sys.getenv("CHECK_FOR_NEW_DATA_INTERVAL_SECONDS"))
  }
}


#' Overwrites the open_database_connection function: Connection data (including the password) must be provided via
#' environment variables.
open_database_connection <- function (...) {
  db_connection <- DBI::dbConnect(
    RPostgres::Postgres(),
    host = Sys.getenv("DB_HOST"),
    port = Sys.getenv("DB_PORT"),
    user = Sys.getenv("DB_USER"),
    password = Sys.getenv("DB_PASSWORD"),
    dbname = Sys.getenv("DB_DBNAME")
  )
  return(db_connection)
}


smtp <- emayili::server(
  host = Sys.getenv("EMAILS_SENDER_SMTP_HOST"),
  port = Sys.getenv("EMAILS_SENDER_SMTP_PORT"),
  username = Sys.getenv("EMAILS_SENDER_SMTP_USERNAME"),
  password = Sys.getenv("EMAILS_SENDER_SMTP_PASSWORD")
)
email_recipients <- str_split(Sys.getenv("EMAILS_RECIPIENTS"), ",")[[1]]

send_email <- function (subject, text) {
  if (!as.logical(Sys.getenv("EMAILS_ACTIVATED"))) {
    return()
  }

  subject <- paste("[Harvester]", subject)
  # 10.20.8.56 is the IP address of bs-stadler01
  text <- paste0(text, "\n\nWarm greetings from 10.20.8.56,\nYour virus-free Harvester")

  email <- emayili::envelope() %>%
    emayili::from(Sys.getenv("EMAILS_SENDER_EMAIL")) %>%
    emayili::to(email_recipients) %>%
    emayili::subject(subject) %>%
    emayili::text(text)
  smtp(email, verbose = FALSE)
}


get_state <- function (db_connection, program_name) {
  pn <- program_name
  data <- tbl(db_connection, "automation_state") %>%
    filter(program_name == pn) %>%
    collect()
  return(data$state)
}


set_state <- function (db_connection, program_name, state) {
  update_statement <- DBI::dbSendQuery(
    db_connection,
    "insert into automation_state (program_name, state)
    values ($1, $2)
    on conflict (program_name) do
      update set state = $2;",
    list(
      program_name,
      state
    )
  )
  DBI::dbClearResult(update_statement)
}
