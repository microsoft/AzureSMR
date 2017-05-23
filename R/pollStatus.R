pollStatusTemplate <- function(azureActiveContext, deplname, resourceGroup) {
  message("Request Submitted: ", Sys.time())
  message("Key: A - accepted, (.) - waiting, U - updating, S - success, F - failure")
  iteration <- 0
  waiting <- TRUE
  while (iteration < 500 && waiting) {
    status <- azureDeployStatus(azureActiveContext, deplname = deplname, resourceGroup = resourceGroup)
    summary <- azureDeployStatusSummary(status)
    rc <- switch(tolower(summary),
        succeeded = "S",
        error = "F",
        failed = "F",
        running = ".",
        updating = "U",
        accepted = "A",
        "?"
      )

    if (rc == "S") {
      waiting = FALSE
    }
    if (rc == "F") {
      message("F")
      warning(paste("Error deploying: ", Sys.time()))
      warning(fromJSON(status$properties$error$details$message)$error$message)
      return(FALSE)
    }
    if (rc == "?") message(status)

    message(rc, appendLF = FALSE)
    iteration <- iteration + 1
    if (rc != "S") Sys.sleep(5)
    }
  return(TRUE)
}



pollStatusVM <- function(azureActiveContext) {

  message("Request Submitted: ", Sys.time())
  message("Key: R - running, (.) - deallocating, D - deallocated, + - starting, S - stopped")
  iteration <- 0
  waiting <- TRUE
  while (iteration < 500 && waiting) {
    status <- azureVMStatus(azureActiveContext, ignore = "Y")
    summary <- gsub(".*?(deallocated|deallocating|running|updating|starting|stopped|NA).*?", "\\1", status)
    rc <- switch(tolower(summary),
      running = "R",
      deallocating = ".",
      deallocated = "D",
      updating = "U",
      starting = "+",
      stopped = "S",
      na = "X",
      "?"
      )

    if (rc %in% c("S", "D", "X")) {
      waiting = FALSE
    }
    if (rc == "?") message(status)

    message(rc, appendLF = FALSE)
    iteration <- iteration + 1
    if (!rc %in% c("S", "D", "X")) Sys.sleep(5)
    }
  message("")
  return(TRUE)
}
