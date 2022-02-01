library(sys)


SERVER <- "euler.ethz.ch"
USER <- "[USER]"
UPLOADS_FOLDER <- "/cluster/project/pangolin/working/uploads"
PRIVATE_KEY_EULER <- "id_ed25519_euler"
PASSPHRASE <- "[TOBEFILLEDIN]"

MAX_CONN <- 10
MAX_SAMPLES_PER_CALL <- 200

SSH <- Sys.which("ssh")

upload_raw_data_files <- function(samples, date) {

    n_samples <- length(samples)

    r1_files <- list()
    r2_files <- list()

    n_chunks <- ceiling(length(samples) / MAX_SAMPLES_PER_CALL)
    chunks <- split(1:n_samples, (1:n_samples) %% n_chunks)
    pids <- rep(-1, MAX_CONN);
    std_outs <- rep(-1, MAX_CONN);
    std_errs <- rep(-1, MAX_CONN);

    for (i in 1:length(chunks)) {

        j <- (i %% MAX_CONN) + 1

        # if connection j was spawned already, wait for finish:
        if (pids[j] > -1) {
            exec_status(pids[j], wait=TRUE)
            results <- collect_results(std_outs[j], std_errs[j])
            r1_files <- c(r1_files, results$r1_files)
            r2_files <- c(r2_files, results$r2_files)
            pids[j] <- -1;
        }

        chunk <- samples[unlist(chunks[i])]
        handle <- spawn_ssh_process(date, chunk)
        pids[j] <- handle$pid
        std_outs[j] <- handle$stdout
        std_errs[j] <- handle$stderr
    }

    # collect remaining results
    for (j in 1:MAX_CONN) {
        if (pids[j] > -1) {
            exec_status(pids[j], wait=TRUE)
            results <- collect_results(std_outs[j], std_errs[j])
            r1_files <- c(r1_files, results$r1_files)
            r2_files <- c(r2_files, results$r2_files)
            pids[j] <- -1;
        }
    }
    list(r1_files=r1_files, r2_files=r2_files)
}


spawn_ssh_process <- function(date, samples)
{
    args = c(paste0(USER, "@", SERVER),
                "-i", PRIVATE_KEY_EULER,
                "-o", "StrictHostKeyChecking=accept-new",
                "spsp",
                UPLOADS_FOLDER,
                date,
                PASSPHRASE,
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
            sample_id <- fields[2];
            filename <- fields[3];
            if (grepl("_R1.", filename, fixed=TRUE))
                r1_files[[sample_id]] <- basename(filename)
            else
                r2_files[[sample_id]] <- basename(filename)
        }
    }
    return(list(r1_files=r1_files, r2_files=r2_files))
}
