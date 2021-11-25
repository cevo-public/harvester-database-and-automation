library(DBI)
# library(odbc)

DB_UTILS <- "R/utility.R"
DB_PASSMETHOD <- "askpass"

source(DB_UTILS)

con <- open_database_connection(password_method = DB_PASSMETHOD)

# Update the data by creating a staging table
# https://stackoverflow.com/questions/49603138/r-updating-database-with-dbi
run_code <- function() {
  if (DBI::dbExistsTable(con, "iris")) {
    DBI::dbRemoveTable(con, "iris")
  }
  if (DBI::dbExistsTable(con, "iris_staging")) {
    DBI::dbRemoveTable(con, "iris_staging")
  }

  colnames(iris) <- c("sepal_length", "sepal_width", "petal_length", "petal_width", "species")

  # create and populate a table (adding the row names as a separate columns used as row ID)
  DBI::dbWriteTable(con, "iris", iris, row.names = TRUE)

  # create a modified version of the table
  iris2 <- iris
  iris2[2] <- 100
  iris2 <- rbind(iris2, c(1.1, 1.1, 1.1, 1.1, "setosa"))
  iris2$row_names <- c(rownames(iris), nrow(iris) + 1)

  # create staging table
  DBI::dbWriteTable(con, "iris_staging", iris2)

  # Append columns row_names and sepal_width from iris_staging for rows that only exist in iris_staging
  # I get an error if I don't cast the types of the selected columns
  append_sql <- "INSERT INTO iris (row_names, sepal_width)
    SELECT s.row_names::float, s.sepal_width::float FROM iris_staging s
    WHERE NOT EXISTS
    (SELECT 1 FROM iris t
    WHERE t.row_names = s.row_names)"
  DBI::dbSendQuery(con, append_sql)

  # Left-join table and staging table (so, add new staging table results) and update the table based on this
  update_sql <- "UPDATE iris
    SET sepal_width = s.sepal_width::float
    FROM iris t
    LEFT JOIN iris_staging s ON t.row_names = s.row_names"
  dbSendQuery(con, update_sql)
}

run_code()

# Update the data without creating a staging table
# https://stackoverflow.com/questions/20546468/how-to-pass-data-frame-for-update-with-r-dbi/43642590#43642590
run_code_2 <- function() {
  if (DBI::dbExistsTable(con, "iris")) {
    DBI::dbRemoveTable(con, "iris")
  }

  colnames(iris) <- c("sepal_length", "sepal_width", "petal_length", "petal_width", "species")

  # create and populate a table (adding the row names as a separate columns used as row ID)
  DBI::dbWriteTable(con, "iris", iris, row.names = TRUE)

  # create a modified version of the table
  iris2 <- iris
  iris2$sepal_length <- 5
  iris2$sepal_width[2] <- 1
  iris2$row_names <- rownames(iris)  # use the row names as unique row ID

  update_sql <- glue::glue_sql('update iris set sepal_length=$1, sepal_width=$2, petal_length=$3, petal_width=$4, species=$5 WHERE row_names=$6')
  update <- DBI::dbSendQuery(con, update_sql)

  DBI::dbBind(update, iris2)  # send the updated data

  DBI::dbClearResult(update)  # release the prepared statement
}

run_code_2()
