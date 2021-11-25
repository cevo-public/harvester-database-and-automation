# Functions to produce JSON-formatted log entries according to a simple homemade schema:
# {
# type: 'log' or 'notification'
#   timestamp:
#   function:
#   payload: {
#     priority: 'INFO', 'WARN', or 'ERROR'
#       message:
#   }
# }

require(jsonlite)

#' Helper function to generate JSON-formatted log entry of priority INFO.
log.info <- function(msg, fcn) {
  json_entry <- get_json_entry(
    type = "log",
    priority = "INFO",
    msg = msg,
    fcn = fcn
  )
  return(json_entry)
}

#' Helper function to generate JSON-formatted log entry of priority WARN.
log.warn <- function(msg, fcn) {
  json_entry <- get_json_entry(
    type = "log",
    priority = "WARN",
    msg = msg,
    fcn = fcn
  )
  return(json_entry)
}

#' Helper function to generate JSON-formatted log entry of priority ERROR.
log.error <- function(msg, fcn) {
  json_entry <- get_json_entry(
    type = "log",
    priority = "ERROR",
    msg = msg,
    fcn = fcn
  )
  return(json_entry)
}

#' Helper function to generate JSON-formatted notification of priority WARN
notify.warn <- function(msg, fcn) {
  json_entry <- get_json_entry(
    type = "notification",
    priority = "WARN",
    msg = msg,
    fcn = fcn
  )
  return(json_entry)
}

#' Helper function to generate JSON-formatted notification of priority ERROR.
notify.error <- function(msg, fcn) {
  json_entry <- get_json_entry(
    type = "notification",
    priority = "ERROR",
    msg = msg,
    fcn = fcn
  )
  return(json_entry)
}

#' Generate JSON-formatted log entry. 
#' @param type One of 'notification' or 'log'
#' @param priority One of 'INFO', 'WARN', or 'ERROR'.
#' @param fcn Character giving the calling function, e.g. "spsp_exporter.R::fetch_data".
#' @param script_name Character giving the script name, e.g. "spsp_exporter.R".
#' @param msg Character message, e.g. "Data not found."
#' @return JSON-formatted log entry, e.g. {"timestamp":["Tue May 18 09:45:25 2021"],"priority":["ERROR"],"function":["spsp_exporter.R::fetch_data"],"payload":["Data not found."]} 
get_json_entry <- function(type, priority, fcn = NULL, msg, script_name = NULL) {
  # Check input
  types <- c("notification", "log")
  priorities <- c('INFO', 'WARN', 'ERROR')
  if (!(priority %in% priorities)) {
    get_json_entry(
      type = "log",
      priority = "ERROR", 
      msg = paste("Specified priority not one of", paste0(priorities, collapse = ", ")),
      script_name = script_name,
      fcn = fcn)
  } else if (!(type %in% types)) {
    get_json_entry(
      type = "log",
      priority = "ERROR", 
      msg = paste("Specified type not one of", paste0(types, collapse = ", ")),
      script_name = script_name,
      fcn = fcn)
  }
  if (!(is.null(script_name))) {
    fcn <- paste(
      script_name, "::", 
      gsub(pattern = "\\(.*\\)", replacement = "", x = deparse(sys.call(-1))), 
      sep = "")
  }
  
  # Create log entry
  entry <- list(
    "timestamp" = get_timestamp(), 
    "type" = type,
    "function" = fcn,
    "payload" = list(
      "priority" = priority, 
      "payload" = msg
    )
  )
  json_entry <- jsonlite::toJSON(x = entry)
  return(json_entry)
}

#' Generate a timestamp to be used in log entries.
#' @return Character timestamp, e.g. "Tue May 18 09:37:44 2021". 
get_timestamp <- function() {
  stamp <- timestamp(prefix = "", suffix = "", quiet = T)
  return(stamp)
}

#' Format a dataframe for log message.
#' @param dataframe A dataframe containing information to log.
#' @return Character.
format_dataframe_for_log <- function(dataframe) {
  text_for_log <- paste0(apply(
    dataframe, 
    1, paste, collapse = " - "),
    collapse = "\n")
  return(text_for_log)
}
