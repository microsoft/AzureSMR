#' Get Status of a HDI Hive Service / version.
#'
#' @inheritParams setAzureContext
#' @inheritParams azureAuthenticate
#'
#' @importFrom httr authenticate
#'
#' @family Hive functions
#' @export
azureHiveStatus <- function(azureActiveContext, clustername, hdiAdmin,
                            hdiPassword, verbose = FALSE) {
  HA = ""
  if (missing(clustername)) {
    CN <- azureActiveContext$clustername
  } else (CN = clustername)
  if (missing(hdiAdmin)) {
    HA <- azureActiveContext$hdiAdmin
  } else (HA = hdiAdmin)
  if (missing(hdiPassword)) {
    HP <- azureActiveContext$hdiPassword
  } else (HP = hdiPassword)


  if (!length(CN)) {
    stop("Error: No Valid clustername provided")
  }
  if (!length(HA)) {
    stop("Error: No Valid hdiAdmin provided")
  }
  if (!length(HP)) {
    stop("Error: No Valid hdiPassword  provided")
  }

  azureActiveContext$hdiAdmin <- HA
  azureActiveContext$hdiPassword <- HP
  azureActiveContext$clustername <- CN

  uri <- paste0("https://", CN, ".azurehdinsight.net/templeton/v1/status")

  r <- GET(URL, add_headers(.headers = c(`Content-type` = "application/json")),
           authenticate(HA, HP), verbosity)
  if (status_code(r) != 200 && status_code(r) != 201) {
    stop(paste0("Error: Return code(", status_code(r), ")"))
  }

  rl <- content(r, "text", encoding = "UTF-8")
  df <- fromJSON(rl)
  return(paste("Status:", df$status, " version:", df$version))

}


#' Submit SQL command to Hive Service.
#'
#' @inheritParams setAzureContext
#' @inheritParams azureAuthenticate
#'
#' @param CMD SQl COmmand String
#' @param path path
#'
#' @family Hive functions
#' @export
azureHiveSQL <- function(azureActiveContext, CMD, clustername, hdiAdmin,
                         hdiPassword, path = "wasb:///tmp/", verbose = FALSE) {
  HA = ""
  if (missing(clustername)) {
    CN <- azureActiveContext$clustername
  } else (CN = clustername)
  if (missing(hdiAdmin)) {
    HA <- azureActiveContext$hdiAdmin
  } else (HA = hdiAdmin)
  if (missing(hdiPassword)) {
    HP <- azureActiveContext$hdiPassword
  } else (HP = hdiPassword)
  if (missing(CMD)) {
    stop("Error: No Valid Command(CMD) provided")
  }
  verbosity <- set_verbosity(verbose)


  if (!length(CN)) {
    stop("Error: No Valid clustername provided")
  }
  if (!length(HA)) {
    stop("Error: No Valid hdiAdmin provided")
  }
  if (!length(HP)) {
    stop("Error: No Valid hdiPassword  provided")
  }

  azureActiveContext$hdiAdmin <- HA
  azureActiveContext$hdiPassword <- HP
  azureActiveContext$clustername <- CN

  # bodyI <- list(user.name = HA, execute = CMD, statusdir='wasb:///tmp/'
  # ) bodyI <- '{execute=\'show
  # tables\';statusdir=\'HiveJobStatusFeb3\';enabloelog=\'false\';}'

  bodyI <- paste("user.name=", HA, "&execute=", CMD, "&statusdir=", path,
                 sep = "")
  # print(bodyI) bodyI <- 'user.name=admin&execute=SHOW
  # TABLES&statusdir=wasb:///tmp/' print(bodyI)

  URL <- paste("https://", CN, ".azurehdinsight.net/templeton/v1/hive?user.name=",
               HA, "&execute=", CMD, "&statusdir=wasb:///tmp/", sep = "")
  URL <- paste("https://", CN, ".azurehdinsight.net/templeton/v1/hive?user.name=",
               HA, sep = "")

  r <- POST(URL, add_headers(.headers = c(`Content-type` = "application/x-www-form-urlencoded")),
            authenticate(HA, HP), body = bodyI, encode = "json", verbosity)
  # ,authenticate('admin', 'Summer2014!')
  rl <- content(r, "text", encoding = "UTF-8")
  df <- fromJSON(rl)
  # print(df$id)
  URL <- paste("https://", CN, ".azurehdinsight.net/templeton/v1/jobs/",
               df$id, sep = "")

  Sys.sleep(2)
  r <- GET(URL, add_headers(.headers = c(`Content-type` = "application/json")),
           authenticate(HA, HP))
  rl <- content(r, "text", encoding = "UTF-8")
  df <- fromJSON(rl)

  message(paste("CMD Running: ", Sys.time()))
  message("Prep(P), Running(R), Completed(C)")
  DUR <- 2
  # print(df$status$state)
  while (df$status$state == "RUNNING" | df$status$state == "PREP") {
    Sys.sleep(DUR)
    if (DUR < 5)
      DUR <- DUR + 1
    if (df$status$state == "PREP")
      message("P")
    if (df$status$state == "RUNNING")
      message("R")
    # print(df$status$state)

    r <- GET(URL, add_headers(.headers = c(`Content-type` = "application/json")),
             authenticate(HA, HP))
    rl <- content(r, "text", encoding = "UTF-8")
    rh <- headers(r)
    df <- fromJSON(rl)
  }
  if (df$status$state == "SUCCEEDED")
    message("S")
  if (df$status$state == "FAILED")
    message("F")

  STATE <- df$status$state
  message("Finished Running statement: ", Sys.time())
  return(TRUE)
}
