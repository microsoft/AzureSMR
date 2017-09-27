find_config_json <- function(){
  settingsfile <- system.file("tests/testthat/config.json", package = "AzureSMR")
  if(!file.exists(settingsfile)){
    settingsfile <- "~/.azuresmr/config.json"
  }
  if(file.exists(settingsfile)) settingsfile else NA
}

# This function is used in unit testing to skip tests if the config file is missing
#
skip_if_missing_config <- function(f){
  if(!file.exists(f)) {
    msg <- paste("config.json is missing.
                 To run tests, add a file ~/.azuresmr/config.json containing AzureML keys.",
                 "See ?workspace for help",
                 sep = "\n")
    testthat::skip(msg)
  }
}

skip_if_offline <- function(){
  u <- tryCatch(url("https://mran.microsoft.com"),
                error = function(e)e)
  if(inherits(u, "error")){
    u <- url("http://mran.microsoft.com")
  }
  on.exit(close(u))
  z <- tryCatch(suppressWarnings(readLines(u, n = 1, warn = FALSE)),
                error = function(e)e)
  if(inherits(z, "error")){
    testthat::skip("Offline. Skipping test.")
  }
}

