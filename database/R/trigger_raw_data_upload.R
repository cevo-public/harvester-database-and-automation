library(sys)

source("R/logger.R")


SSH <- Sys.which("ssh")


upload_raw_data <- function(samples, date, config_file="raw-data-upload.yml") {

    log <- function(msg) {
        print(log.info(
            msg = msg,
            fcn = "trigger_raw_data_upload::upload_raw_data"
        ))
    }

    config <- read_yaml(file = config_file)
    check_config(config)

    max_conn <- config$max_conn
    max_samples_per_call <- config$max_samples_per_call

    n_samples <- length(samples)

    r1_files <- list()
    r2_files <- list()

    n_chunks <- ceiling(n_samples / max_samples_per_call)

    log(paste(
        "start upload of raw data files of",
        n_samples,
        "files in",
        n_chunks,
        "chunks"
    ))

    chunks <- split(1:n_samples, (1:n_samples) %% n_chunks)
    pids <- rep(-1, max_conn);
    std_outs <- rep(-1, max_conn);
    std_errs <- rep(-1, max_conn);

    for (i in 1:length(chunks)) {

        j <- (i %% max_conn) + 1

        # if connection j was spawned already, wait for finish:
        if (pids[j] > -1) {
            log("wait for ssh process to finish")
            exec_status(pids[j], wait=TRUE)
            log("collect results from ssh process")
            results <- collect_results(std_outs[j], std_errs[j])
            r1_files <- c(r1_files, results$r1_files)
            r2_files <- c(r2_files, results$r2_files)
            pids[j] <- -1;
        }

        chunk <- samples[unlist(chunks[i])]
        log("spawn ssh process")
        handle <- spawn_ssh_process(config, date, chunk)
        pids[j] <- handle$pid
        std_outs[j] <- handle$stdout
        std_errs[j] <- handle$stderr
    }

    # collect remaining results
    for (j in 1:max_conn) {
        if (pids[j] > -1) {
            log("wait for ssh process to finish")
            exec_status(pids[j], wait=TRUE)
            results <- collect_results(std_outs[j], std_errs[j])
            log("collect results from ssh process")
            r1_files <- c(r1_files, results$r1_files)
            r2_files <- c(r2_files, results$r2_files)
            pids[j] <- -1;
        }
    }
    list(r1_files=r1_files, r2_files=r2_files)
}


check_config <- function(config) {

  required <- c(
    "server", "user", "uploads_folder", "private_key_euler",
    "passphrase", "max_conn", "max_samples_per_call"
  )

  missing <- setdiff(required, names(config))
  unknown <- setdiff(names(config), required)

  errors <- 0
  if (length(missing) > 0) {
    cat(paste(c(
      "error: the following entries in raw-data config are missing:",
      missing,
      "\n"
    )))
    errors <- errors + 1
  }
  if (length(unknown) > 0) {
    cat(paste(c(
      "error: the following entries in raw-data config are not known:",
      unknown,
      "\n"
    )))
    errors <- errors + 1
  }

  for (name in required) {
    value <- config[name]
    if (is.null(value))
      next
    if (length(config[name]) == 0) {
      cat(paste("error: raw-data config entry for", name, "is empty\n"))
      errors <- errors + 1
    }
  }

  for (num_field in c("max_conn", "max_samples_per_call")) {
    value <- config[name]
    if (is.null(value))
      next
    value <- suppressWarnings(as.integer(value));
    if (is.na(value)) {
      cat(paste(
        "error: raw-data config entry for",
        num_filed,
        "is not an integer number\n"
      ))
      errors <- errors + 1
      next
    }
    config[num_field] <- value
  }
  if (errors > 0)
    stop("raw-data config not valid")
}


spawn_ssh_process <- function(config, date, samples)
{
    server <- config$server
    user <- config$user
    uploads_folder <- config$uploads_folder
    private_key_euler <- config$private_key_euler
    passphrase <- config$passphrase

    t <- tempdir();
    tmp_key <- file.path(t, private_key_euler)
    file.copy(private_key_euler, tmp_key)
    Sys.chmod(tmp_key, mode="0600")
    Sys.chmod(t, mode="0700")

    args = c(paste0(user, "@", server),
                "-i", tmp_key,
                "-o", "StrictHostKeyChecking=accept-new",
                "spsp",
                uploads_folder,
                toString(date),
                passphrase,
                samples
            )
    std_out <- tempfile()
    std_err <- tempfile()
    pid <- exec_background(SSH, args, std_out=std_out, std_err=std_err);
    return (list(pid=pid, stdout=std_out, stderr=std_err))
}


collect_results <- function(std_out, std_err) {

    r1_files <- list()
    r2_files <- list()

    for (msg in readLines(std_err))
        cat(paste("stderr: ", msg, "\n"))

    for (line in readLines(std_out)) {
        if (trimws(line) == "")
            next
        cat(paste("stdout: ", line, "\n"))
        fields <- strsplit(line, ' ')[[1]]
        if (fields[1] == "UPLOAD") {
            sample_id <- fields[3];
            filename <- fields[4];
            if (grepl("_R1.", filename, fixed=TRUE))
                r1_files[[sample_id]] <- basename(filename)
            else if (grepl("\\.cram$", filename, fixed=FALSE))
                r1_files[[sample_id]] <- basename(filename)
            else if (grepl("_R2.", filename, fixed=TRUE))
                r2_files[[sample_id]] <- basename(filename)
            else
                stop(paste("no idea how to handle", filename))
        }
    }
    return(list(r1_files=r1_files, r2_files=r2_files))
}
