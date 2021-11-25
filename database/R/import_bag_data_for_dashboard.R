
import_bag_data_for_dashboard <- function (path_to_data_dir, timestamp, db_connection) {
  # Import the BAG Meldeformular data
  meldeformular_path <- paste0(path_to_data_dir, "/",
                               list.files(path_to_data_dir, pattern = "*_FOPH_COVID19_data_extract.csv"))
  meldeformular <- read_delim(
    meldeformular_path, ";",
    locale = readr::locale(encoding = "UTF-8"),
    skip = 1,
    col_names = c(
      "eingang_dt", "fall_dt", "ktn", "altersjahr", "sex",
      "manifestation_dt", "hospitalisation", "hospdatin", "pttod", "pttoddat",
      "grunderkr_diabetes", "grunderkr_cardio", "grunderkr_hypertonie", "grunderkr_resp_chron", "grunderkr_krebs",
      "grunderkr_immunsup", "grunderkr_adipos", "grunderkr_chron_nier", "grunderkr_andere", "grunderkr_keine",
      "icu_aufenthalt", "em_hospit_icu_in_dt",
      "em_hospit_icu_out_dt", "expo_pers_familie", "expo_pers_gemeins", "expo_pers_gesundh", "expo_pers_passagiere",
      "expo_pers_andere", "exp_ort", "exp_land", "exp_land_cd", "exp_von", "exp_bis", "exp_dt", "exp_ausland_von",
      "exp_ausland_bis", "exp_wann_unbek", "exp_enger_kontakt_pos_fall", "exp_kontakt_art", "anzahl_erg", "anzahl_em",
      "quarant_vor_pos", "lab_grund", "lab_grund_txt", "form_version", "variant_of_concern", "typ"
    ),
    col_types = cols(
      eingang_dt = col_date(),
      fall_dt = col_date(),
      ktn = col_character(),
      altersjahr = col_integer(),
      sex = col_character(),
      manifestation_dt = col_date(),
      hospitalisation = col_integer(),
      hospdatin = col_date(),
      pttod = col_logical(),
      pttoddat = col_date(),
      grunderkr_diabetes = col_logical(),
      grunderkr_cardio = col_logical(),
      grunderkr_hypertonie = col_logical(),
      grunderkr_resp_chron = col_logical(),
      grunderkr_krebs = col_logical(),
      grunderkr_immunsup = col_logical(),
      grunderkr_adipos = col_logical(),
      grunderkr_chron_nier = col_logical(),
      grunderkr_andere = col_logical(),
      grunderkr_keine = col_logical(),
      icu_aufenthalt = col_integer(),
      em_hospit_icu_in_dt = col_date(),
      em_hospit_icu_out_dt = col_date(),
      expo_pers_familie = col_logical(),
      expo_pers_gemeins = col_logical(),
      expo_pers_gesundh = col_logical(),
      expo_pers_passagiere = col_logical(),
      expo_pers_andere = col_logical(),
      exp_ort = col_integer(),
      exp_land = col_character(),
      exp_land_cd = col_character(),
      exp_von = col_date(),
      exp_bis = col_date(),
      exp_dt = col_date(),
      exp_ausland_von = col_date(),
      exp_ausland_bis = col_date(),
      exp_wann_unbek = col_integer(),
      exp_enger_kontakt_pos_fall = col_integer(),
      exp_kontakt_art = col_integer(),
      anzahl_erg = col_integer(),
      anzahl_em = col_integer(),
      quarant_vor_pos = col_integer(),
      lab_grund = col_integer(),
      lab_grund_txt = col_character(),
      form_version = col_character(),
      variant_of_concern = col_character(),
      typ = col_character()
    )
  ) %>% rename(
    confirmed_variant_of_concern_txt = variant_of_concern,
    gen_variant = typ
  )

  # Import the number of positive and negative tests
  test_numbers_path <- paste0(path_to_data_dir, "/",
                                 list.files(path_to_data_dir, pattern = "*_Time_series_tests.csv"))
  test_numbers_akl_path <- paste0(path_to_data_dir, "/",
                                     list.files(path_to_data_dir, pattern = "*_Timeseries_tests_akl.csv"))
  test_numbers <- read_delim(
    test_numbers_path, ";",
    skip = 1,
    col_names = c("date", "positive_tests", "negative_tests"),
    col_types = cols(
      date = col_date(),
      positive_tests = col_integer(),
      negative_tests = col_integer()
    )
  )
  test_numbers_akl <- read_delim(
    test_numbers_akl_path, ";",
    skip = 1,
    na = c("", "NA", "Unbekannt"),
    col_names = c("canton", "date", "age_group", "positive_tests", "negative_tests"),
    col_types = cols(
      canton = col_character(),
      date = col_date(),
      age_group = col_character(),
      positive_tests = col_integer(),
      negative_tests = col_integer()
    )
  )
  min_akl_date <- min(test_numbers_akl$date)
  test_numbers <- test_numbers %>%
    filter(date < min_akl_date) %>%
    mutate(canton = NA, age_group = NA)
  test_numbers_merged <- rbind(test_numbers, test_numbers_akl)


  # Write to database
  DBI::dbBegin(db_connection)

  delete_statement <- DBI::dbSendStatement(db_connection, "delete from bag_test_numbers;")
  DBI::dbClearResult(delete_statement)
  DBI::dbAppendTable(db_connection, "bag_test_numbers", test_numbers_merged)

  delete_statement <- DBI::dbSendStatement(db_connection, "delete from bag_dashboard_meldeformular;")
  DBI::dbClearResult(delete_statement)
  DBI::dbAppendTable(db_connection, "bag_dashboard_meldeformular", meldeformular)

  update_statement <- DBI::dbSendQuery(
    db_connection,
    'update dashboard_state set last_data_update = $1;',
    timestamp
  )
  DBI::dbClearResult(update_statement)

  DBI::dbCommit(db_connection)
}


get_newest_bag_data_for_dashboard_dir <- function (base_data_dir) {
  sub_dirs <- list.files(base_data_dir, pattern = "[0-9]{4}-[0-9]{2}-[0-9]{2}")
  newest <- max(as.Date(sub_dirs))
  full_path <- file.path(base_data_dir, newest)
  if (!dir.exists(full_path)) {
    stop(paste("Unexpected error: ", file.path(base_data_dir, newest), "is not a directory"))
  }
  return(list(
    path = full_path,
    timestamp = newest
  ))
}


# dashboard_bag_data_info <- get_newest_bag_data_for_dashboard_dir("<path>")
# db_connection <- open_database_connection()
# dashboard_state <- tbl(db_connection, "dashboard_state") %>% collect()
# if (identical(dashboard_state$last_data_update, dashboard_bag_data_info$timestamp)) {
#   print(paste0(Sys.time(), ": No new data available. Current data remains ", dashboard_bag_data_info$timestamp))
# } else {
#   print(paste0(Sys.time(), ": Found new data: ", dashboard_bag_data_info$timestamp, ". Start updating."))
#   import_bag_data_for_dashboard(dashboard_bag_data_info$path, dashboard_bag_data_info$timestamp, db_connection)
#   print(paste0(Sys.time(), ": Update finished. Current data: ", dashboard_bag_data_info$timestamp))
# }
# DBI::dbDisconnect(db_connection)
