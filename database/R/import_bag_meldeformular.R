#' This script is to get Meldeformular data provided to us by the BAG into a standard
#' format for our database.
#'
#' Need to watch out for memory leakage where db_connection keeps all the data around and piling up when we re-use in each iteration of a loop
#'
#' @param db_connection
#' @param bag_metadata_dir
#' @param n_rows Maximum number rows to import from newest raw data file (for testing purposes). Default: import all rows.
#' @param tbl_name Name of database table to import to. Default: bag_meldeformular.
#' @param rawdata_filepath Path to specific raw data file to import. Default: NULL means most recent file will be imported.
#' @param verbose Be even more verbose with printed output.
#' @param part_of_automation Whether the function is called by the automated pipeline. If yes, it will check the
#'    the state stored in the automation_state table and only import data if a new file is available.
import_bag_meldeformular <- function (
  db_connection, bag_metadata_dir, part_of_automation = FALSE, n_rows = Inf,
  tbl_name = "bag_meldeformular", rawdata_filepath = NULL,
  verbose = F
) {
  start_time <- Sys.time()
  RENAME_COLS <- c(
    "sample_number" = "labor_auftragsnummer",
    "kanton" = "ktn",
    "hospitalisation_type" = "hospitalisation",
    "variant_of_concern_typ" = "typ")

  bag_meldeformular_hospitalisation_type <- c(
    "1" = "HOSPITALIZED",
    "2" = "NOT_HOSPITALIZED",
    "3" = "UNKNOWN",
    "NA" = "NOT_FILLED")

  bag_meldeformular_exp_ort_type <- c(
    "1" = 'SWITZERLAND',
    "2" = 'ABROAD',
    "3" = 'SWITZERLAND_AND_ABROAD',
    "4" = 'UNKNOWN',
    "5" = 'NOT_FILLED',
    "100" = "SWITZERLAND",  # likely an error indicating Switzerland, since Switzerland country code is 100
    "NA" = "NOT_FILLED")

  bag_meldeformular_exp_enger_kontakt_pos_fall_type <- c(
    "1" = 'HAD_CLOSE_CONTACT',
    "2" = 'DID_NOT_HAVE_A_CLOSE_CONTACT',
    "3" = 'UNKNOWN',
    "4" = 'NOT_FILLED',
    "7" = "NOT_FILLED",  # likely a coding error, only applies to one sample as of 2020-11-30
    "NA" = "NOT_FILLED")

  bag_meldeformular_exp_kontakt_art_type <- c(
    "1" = 'FAMILY_MEMBER',
    "2" = 'AS_MEDICAL_STAFF',
    "3" = 'OTHER',
    "4" = 'UNKNOWN',
    "5" = 'SCHOOL_OR_CHILD_CARE',
    "6" = 'WORK',
    "7" = 'PRIVATE_PARTY',
    "8" = 'DISCO_OR_CLUB',
    "9" = 'BAR_OR_RESTAURANT',
    "10" = 'DEMONSTRATION_OR_EVENT',
    "11" = 'SPONTANEOUS_CROWD_OF_PEOPLE',
    "NA" = 'NOT_FILLED')

  bag_meldeformular_lab_grund_type <- c(
    "1" = 'SYMPTOMS',
    "2" = 'OUTBREAK_INVESTIGATION',
    "3" = 'OTHER',
    "4" = 'SWISS_COVID_APP',
    "Symptome kompatibel mit COVID-19" = 'SYMPTOMS',
    "Ausbruchsuntersuchung" = 'OUTBREAK_INVESTIGATION',
    "anderer" = 'OTHER',
    "SwissCovidApp" = 'SWISS_COVID_APP',
    "15092020" = 'OTHER', "18092020" = 'OTHER', "14102020" = 'OTHER',  # unknown coding for these cases
    "NA" = "NOT_FILLED",
    'NOT_FILLED' = 'NOT_FILLED')

  bag_meldeformular_quarant_vor_pos_type <- c(
    "1" = 'QUARANTINE',
    "2" = 'NO_QUARANTINE',
    "3" = 'UNKNOWN',
    "NA" = 'NOT_FILLED',
    "5" = 'UNKNOWN')

  bag_meldeformular_icu_aufenthalt_type <- c(
    "1" = "ICU",
    "2" = "NO_ICU",
    "NA" = "UNKNOWN")

  bag_meldeformular_impfstatus_type <- c(
    "1" = "YES",
    "2" = "NO",
    "3" = "UNKNOWN"
  )

  # Load table specification
  table_spec <- parse_table_specification(
    table_name = tbl_name, db_connection = db_connection)
  col_spec <- parse_column_specification(
    table_name = tbl_name, db_connection = db_connection)

  # Get available raw data files
  raw_data_filepaths <- list.files(
    path = bag_metadata_dir, full.names = T, pattern = ".xlsx")

  # If this is a part of the automated pipeline, data will only be imported when the number of available files has
  # increased. The number of files will be stored as state.
  current_state <- paste0("number_raw_data_files=", length(raw_data_filepaths))
  if (part_of_automation) {
    state <- get_state(db_connection, "import_bag_meldeformular")
    if (identical(current_state, state)) {
      print("Nothing to do. Leaving.")
      return()
    }
  }

  # Load most recent data table
  file_priorities <- getfilename_priority_bag_meldeformular(
    filenames = raw_data_filepaths)
  if (is.null(rawdata_filepath)) {
    filepath <- names(file_priorities)[file_priorities == 1]
  } else {
    filepath <- rawdata_filepath
  }

  print(paste("Loading ", n_rows, "rows (in chunks) from metadata file", filepath))
  load_start_time <- Sys.time()

  # Make sure that the columns are correctly typed and that the date columns get interpreted correctly
  colnames <- colnames(readxl::read_excel(path = filepath, n_max = 1))
  bm_col_types <- rep("text", length(colnames))
  dt_cols <- grepl(x = colnames, pattern = "_dt|hospdatin|pttoddat|exp_von|exp_bis|exp_ausland_von|exp_ausland_bis|impfdatum_dose1|impfdatum_dose2")
 dt_colnames <- colnames[dt_cols]
  print(paste("These columns interpreted as date columns:", paste0(dt_colnames, collapse = ", ")))
  non_dt_colnames <- colnames[!(colnames %in% dt_cols)]
  print(paste("These columns interpreted to NOT be date columns:", paste0(non_dt_colnames, collapse = ", ")))
  print(paste("Note: column 'viol_sample_date' is actually a text column."))
  bm_col_types[dt_cols] <- "date"
  numeric_cols <- grepl(x = colnames, pattern = "dosen_anzahl")
  bm_col_types[numeric_cols] <- "numeric"

  # Process file in chunks
  processed_all_lines <- F
  lines_processed <- 0
  skip <- 1
  chunk_size <- 10000

  while (!processed_all_lines & lines_processed < n_rows) {
    range <- paste0("A", skip + 1, ":", "BO", skip + chunk_size)
    print(paste("Processing data range:", range))

    # Read in chunk_size lines
    data_table <- readxl::read_excel(
      path = filepath,
      range = cellranger::cell_limits(ul = c(skip + 1, 1), lr = c(skip + chunk_size, 67)),  # can't just use skip and chunk_size as n_max because can't count on all chunks having at least 1 value in the last column
      col_types = bm_col_types,
      col_names = colnames) %>%
      filter(if_any(everything(), ~ !is.na(.))) %>%  # filter out all NA rows (cell_limits will just keep taking empty rows at end of sheet)
      mutate(filename = gsub(
        x = filepath,
        pattern = bag_metadata_dir,
        replacement = ""))

    # Set up next iteration already
    if (nrow(data_table) != chunk_size) {
      processed_all_lines <- T
      lines_processed <- lines_processed + nrow(data_table)
    } else {
      lines_processed <- lines_processed + chunk_size
    }
    skip <- skip + chunk_size

    # Transform table
    ignored_cols <- colnames(data_table)[!(colnames(data_table) %in% c(table_spec$name, RENAME_COLS))]
    if (length(ignored_cols) > 0) {
      warning(
        "These columns in the raw data will be ignored:\n",
        paste0(ignored_cols, collapse = "\n"))
    }
    rename_cols_in_table <- RENAME_COLS[RENAME_COLS %in% colnames(data_table)]
    armee_col_present <- "auftraggeber_armee" %in% colnames(data_table)
    comment_col_present <- "comment" %in% colnames(data_table)
    if (armee_col_present & comment_col_present) {
      data_table_transformed <- data_table %>%
        mutate(
          comment = case_when(
            auftraggeber_armee == "TRUE" & (is.na(comment) | comment == "") ~ "auftraggeber_armee=TRUE",
            auftraggeber_armee == "TRUE" ~ paste(as.character(comment), "auftraggeber_armee=TRUE", sep = ";"),
            T ~ as.character(comment)))
    } else if (armee_col_present) {
      data_table_transformed <- data_table %>%
        mutate(
          comment = case_when(
            auftraggeber_armee == "TRUE" ~ "auftraggeber_armee=TRUE"))
    } else {
      data_table_transformed <- data_table
    }

    data_table_transformed <- data_table_transformed %>%
      rename(all_of(rename_cols_in_table)) %>%
      filter(!is.na(sample_number))  # delete rows without primary key

    # if no rows present with primary key, continue
    if (nrow(data_table_transformed) == 0) {
      print("No rows have a value for key column sample_number (labor_auftragsnummer).")
      next
    }

    data_table_transformed <- data_table_transformed %>%
      group_by(sample_number) %>%
      mutate(line_priority = 1:n()) %>%  # for duplicate entries with the same sample number in the same most recent bag file, take last occurance in file
      group_by(sample_number) %>%
      top_n(n = 1, wt = line_priority) %>%
      ungroup()

    data_table_transformed <- data_table_transformed %>%
      mutate(
        iso_country_exp = country_name_to_iso_code(
          country = exp_land,
          language = "german"),
        iso_country_exp = get_standardized_iso_country(
          iso_country = iso_country_exp,
          db_connection = db_connection))

    spec_cols_in_table <- table_spec$name[table_spec$name %in% colnames(data_table_transformed)]
    data_table_transformed <- data_table_transformed %>%
      select(all_of(spec_cols_in_table)) %>%
      ungroup()

    data_table_transformed_recoded <- data_table_transformed %>%
      mutate(
        hospitalisation_type = tidyr::replace_na(
          data = hospitalisation_type,
          replace = bag_meldeformular_hospitalisation_type[["NA"]]),
        hospitalisation_type = recode(
          .x = as.character(hospitalisation_type),
          !!!bag_meldeformular_hospitalisation_type),
        exp_ort = tidyr::replace_na(
          data = exp_ort,
          replace = bag_meldeformular_exp_ort_type[["NA"]]),
        exp_ort = recode(
          .x = as.character(exp_ort),
          !!!bag_meldeformular_exp_ort_type),
        exp_enger_kontakt_pos_fall = tidyr::replace_na(
          data = exp_enger_kontakt_pos_fall,
          replace = bag_meldeformular_exp_enger_kontakt_pos_fall_type[["NA"]]),
        exp_enger_kontakt_pos_fall = recode(
          .x = as.character(exp_enger_kontakt_pos_fall),
          !!!bag_meldeformular_exp_enger_kontakt_pos_fall_type),
        exp_kontakt_art = tidyr::replace_na(
          data = exp_kontakt_art,
          replace = bag_meldeformular_exp_kontakt_art_type[["NA"]]),
        exp_kontakt_art = recode(
          .x = as.character(exp_kontakt_art),
          !!!bag_meldeformular_exp_kontakt_art_type),
        lab_grund = tidyr::replace_na(
          data = lab_grund,
          replace = bag_meldeformular_lab_grund_type[["NA"]]),
        lab_grund = recode(
          .x = as.character(lab_grund),
          !!!bag_meldeformular_lab_grund_type),
        quarant_vor_pos = recode(
          .x = as.character(quarant_vor_pos),
          !!!bag_meldeformular_quarant_vor_pos_type),
        icu_aufenthalt = recode(
          .x = as.character(icu_aufenthalt),
          !!!bag_meldeformular_icu_aufenthalt_type),
        impfstatus = recode(
          .x = as.character(impfstatus),
          !!!bag_meldeformular_impfstatus_type)
      )

    # Update table with data
    key_col <- "sample_number"
    cols_to_update <- colnames(data_table_transformed_recoded)[colnames(data_table_transformed_recoded) != key_col]
    update_table(
      table_name = tbl_name, new_table = data_table_transformed_recoded,
      con = db_connection, append_new_rows = T, key_col = key_col,
      cols_to_update = cols_to_update, table_spec = table_spec)
  }

  time_diff <- Sys.time() - load_start_time
  units(time_diff) <- "secs"
  print(paste("Successfully processed", lines_processed, "lines from file in", signif(time_diff, digits = 2), "seconds."))

  if (part_of_automation) {
    set_state(db_connection, "import_bag_meldeformular", current_state)
    send_email(
      "New BAG metadata imported",
      paste0("I finished importing new BAG metadata from the following files:\n\n",
             paste(raw_data_filepaths, collapse = "\n")))
  }
}
