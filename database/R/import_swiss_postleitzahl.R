# This script is to transform & load Swiss postal code location data to table "swiss_postleitzahl"
# Data source: https://www.bfs.admin.ch/bfs/de/home/statistiken/gesundheit/nomenklaturen/medsreg.assetdetail.11527097.html

require(xlsx)
require(dplyr)


DB_PASSMETHOD <- "askpass"
DB_UTILS <- "R/utility.R"
DATA_LINK <- "https://www.bfs.admin.ch/bfsstatic/dam/assets/11527097/master"
DATA_DEST <- "~/Downloads/MedizinischeStatistikderKrankenhaeuser.xlsx"
TBL_NAME <- "swiss_postleitzahl"
SQL_SPEC <- "database/init.sql"

source(DB_UTILS)

# Download data
download.file(url = DATA_LINK, destfile = DATA_DEST)
data <- readxl::read_xlsx(path = DATA_DEST, sheet = "REGION=CH")

# Format data
swiss_postleitzahl <- data %>%
  rename("plz" = "NPA/PLZ", "region" = "NAME", "canton" = "KT") %>%
  select(plz, region, canton)

# Enforce table specifications,
# if fails returns NA table which can't be appended to database table
swiss_postleitzahl <- tryCatch(
  {
    enforce_sql_spec(
      table = swiss_postleitzahl,
      table_name = TBL_NAME,
      sql_specification = SQL_SPEC)
  },
  error = function(cond) {
    message(cond)
    return(NA)
  }
)

# Connect to database
db_connection <- open_database_connection(password_method = DB_PASSMETHOD)

# Append data to table
DBI::dbBegin(db_connection)
DBI::dbAppendTable(db_connection, name = TBL_NAME, swiss_postleitzahl)
DBI::dbCommit(db_connection)
DBI::dbDisconnect(db_connection)
