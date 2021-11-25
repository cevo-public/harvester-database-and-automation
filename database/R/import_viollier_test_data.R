#' This function reads the data from the "Orders and results" table in the "COVID 19 Orders and results von ...
#' bis ..._ETH_short_BECR.xlsx" sheet and loads them into the database. It expects that the mentioned plates and
#' sample numbers do not exist in the database yet.
#'
#' It reads a CSV file containing a header row with the following headers:
#'  zip_code;city;canton;order_date;sample_number;pcr_code;e_gene_ct;rdrp_gene_ct;plate_name_and_well_position
#' The order_date has to be formatted as dd.mm.yyyy.
#'
#' @param path_to_csv (character)
#' @param db_connection (DBI::DBIConnection)
import_viollier_orders_and_results_sheet <- function (path_to_csv, db_connection, delimiter = ";") {
  # Read data from file
  raw_data <- readr::read_delim(path_to_csv, delimiter, locale = locale(encoding = "UTF-8"), col_types = cols(
    zip_code = col_character(),
    city = col_character(),
    canton = col_character(),
    order_date = col_date("%d.%m.%Y"),
    sample_number = col_integer(),
    pcr_code = col_character(),
    e_gene_ct = col_character(),
    rdrp_gene_ct = col_character(),
    plate_name_and_well_position = col_character(),
    sequenced_by_viollier = col_logical()
  ))

  # Cleaning
  cleaned <- raw_data %>%
    mutate(
      e_gene_ct = suppressWarnings(as.integer(e_gene_ct)),
      rdrp_gene_ct = suppressWarnings(as.integer(rdrp_gene_ct))
    ) %>%
    mutate(
      e_gene_ct = ifelse(e_gene_ct > 0, e_gene_ct, NA),
      rdrp_gene_ct = ifelse(rdrp_gene_ct > 0, rdrp_gene_ct, NA)
    ) %>%
    group_by(sample_number) %>%
    summarize(
      zip_code = first(zip_code),
      city = first(city),
      canton = first(canton),
      order_date = first(order_date),
      pcr_code = first(pcr_code),
      e_gene_ct = first(e_gene_ct),
      rdrp_gene_ct = first(rdrp_gene_ct),
      plate_name_and_well_position = first(plate_name_and_well_position),
      sequenced_by_viollier = case_when(
        any(sequenced_by_viollier) ~ T,
        T ~ F)  # can't be null
    )

  # Creates the join table.
  # The plate_name_and_well_position column might contain multiple plate/well pairs, e.g.,
  # "123456eg78-A1  123456eg78-A2".
  viollier_test__viollier_plate_tbl <- cleaned %>%
    select(sample_number, plate_name_and_well_position, e_gene_ct, rdrp_gene_ct) %>%
    separate_rows(plate_name_and_well_position, sep = "\\s+") %>%
    distinct() %>%
    mutate(
      plate_name_and_well_position = ifelse(str_detect(plate_name_and_well_position, "-"),
                                            plate_name_and_well_position, NA)
    ) %>%
    drop_na(plate_name_and_well_position) %>%
    separate(plate_name_and_well_position, into = c("viollier_plate_name", "well_position"), sep = "-") %>%
    mutate(
      viollier_plate_name = tolower(viollier_plate_name)
    )

  # Creates the plate table
  viollier_plate_tbl <- viollier_test__viollier_plate_tbl %>%
    select(viollier_plate_name) %>%
    distinct()

  # Creates the test table
  viollier_test_tbl <- cleaned %>%
    select(sample_number, order_date, zip_code, city, canton, pcr_code, sequenced_by_viollier) %>%
    drop_na(sample_number) %>%
    mutate(
      is_positive = replace_na(pcr_code == "4", FALSE)
    )

  # Some plates might already exist since some plates contains samples referenced from sheets from two different
  # adjecent weeks. This loads the plate names that are already present in the database and exclude them from the plates
  # that should be inserted.
  old_viollier_plate_table <- tbl(db_connection, "viollier_plate") %>%
    select(viollier_plate_name) %>%
    collect
  viollier_plate_tbl <- viollier_plate_tbl %>%
    anti_join(old_viollier_plate_table, by = "viollier_plate_name")

  # Insert the data in a single transaction
  DBI::dbBegin(db_connection)
  DBI::dbAppendTable(db_connection, name = "viollier_plate", viollier_plate_tbl)
  DBI::dbAppendTable(db_connection, name = "viollier_test", viollier_test_tbl)
  DBI::dbAppendTable(db_connection, name = "viollier_test__viollier_plate", viollier_test__viollier_plate_tbl)
  DBI::dbCommit(db_connection)
}


#' This function takes the data from the two Biorad PRC machines that are disconnected from the network. The results
#' from them are not included in the weekly-received Excel sheets. The plates are named "...wuhan...".
#'
#' The code was initially written by seidels.
import_biorad_wuhan_plates <- function (data_dir, db_connection) {
  out <- data.frame(sample_number = -1, viollier_plate_name = "?", well = -1)
  dirs <- list.dirs(data_dir, recursive = FALSE, full.names = FALSE)

    # for all files, do
  for (iDir in dirs) {
    files <- list.files(paste0(data_dir, "/", iDir, "/QuantStep4/"), pattern = "*Quantitation Summary*")

    for (iFile in files) {
      # load data
      fPath <- paste0(data_dir, "/", iDir, "/QuantStep4/", iFile)

      dat <- xlsx::read.xlsx(fPath, sheetIndex = 1)

      samplepositions <- unique(dat$Well)
      sampleNames <- as.character(unique(dat$Sample))

      assertthat::are_equal(length(samplepositions), length(sampleNames))

      #get only correct sample names
      sampleNames <- sapply(sampleNames, function(x) {
        check_sample_name_format(x)
      })

      sampleNames <- sapply(sampleNames, function(x) {
        as.character(check_sample_name_content(x))
      })
      sampleNames <- as.character(sampleNames)

      ids <- which(sampleNames != -1)

      if (length(ids) != 0) {
        outSub <- data.frame(sample_number = sampleNames[ids], well = samplepositions[ids])
        outSub$viollier_plate_name <- iDir
        out <- rbind(out, outSub)
      }
    }
  }

  out <- out[2:nrow(out),] %>%
    mutate(viollier_plate_name = tolower(viollier_plate_name)) %>%
    distinct(sample_number, viollier_plate_name, well) %>%
    filter(sample_number != -1) %>%
    drop_na(sample_number, viollier_plate_name, well)

  DBI::dbBegin(db_connection)
  existing_sample_numbers <- tbl(db_connection, "viollier_test") %>%
    select(sample_number) %>%
    collect()
  out <- out %>%
    mutate(sample_number = as.integer(sample_number)) %>%
    inner_join(existing_sample_numbers)

  insert_plates_query <- DBI::dbSendQuery(
    db_connection,
    "insert into viollier_plate (viollier_plate_name) values( $1 ) on conflict do nothing;",
    list(unique(out$viollier_plate_name))
  )
  DBI::dbClearResult(insert_plates_query)

  insert_test_and_plate_query <- DBI::dbSendQuery(
    db_connection,
    "insert into viollier_test__viollier_plate (sample_number, viollier_plate_name, well_position)
    values ($1, $2, $3)
    on conflict do nothing;",
    list(
      out$sample_number,
      out$viollier_plate_name,
      out$well
    )
  )
  DBI::dbClearResult(insert_test_and_plate_query)
  DBI::dbCommit(db_connection)
}


#' Helper function for import_biorad_wuhan_plates
#' Check that the sample name entry contains only the sample name and not some additional input
#'
#' The code was initially written by seidels.
check_sample_name_format <- function(rawSampleName) {
  splitted_blank <- strsplit(rawSampleName, split = " ")
  splitted_hyphen <- strsplit(rawSampleName, split = "-")
  splitted_dot <- strsplit(rawSampleName, split = ".", fixed = TRUE)

  if (length(splitted_blank[[1]]) == 2) {
    return(splitted_blank[[1]][1])
  } else if (length(splitted_blank[[1]]) > 2) {
    print(rawSampleName)
    return(-1)
    stop("Error. Sample Name consists of more than 2 elements!")
  } else if (length(splitted_hyphen[[1]]) == 2) {
    return(splitted_hyphen[[1]][1])
  } else if (length(splitted_hyphen[[1]]) > 2) {
    print(rawSampleName)
    return(-1)
  } else if (length(splitted_dot[[1]]) == 2) {
    return(splitted_dot[[1]][1])
  } else if (length(splitted_dot[[1]]) > 2) {
    print(rawSampleName)
    return(-1)
  } else {
    return(rawSampleName)
  }
}


#' Helper function for import_biorad_wuhan_plates
#' Check that sample name is numeric and contains 8 digits
#'
#' The code was initially written by seidels.
check_sample_name_content <- function(raw_sample_name) {
  # check for numeric value
  if (!str_detect(raw_sample_name, "^[0-9]+$")) {
    # This is probably NOT an error. And if it is an error, there is nothing what we can do about.
    return(-1)
  }

  numeric_value <- as.numeric(raw_sample_name)

  # check for 8 digits
  if (nchar(numeric_value) == 8) {
    return(numeric_value)
  } else if (nchar(numeric_value) == 10) {
    return(numeric_value %% 100000000)
  } else {
    print(paste("Found invalid sample name (not 8 and not 10 digits):", raw_sample_name))
    return(-1)
  }
}
