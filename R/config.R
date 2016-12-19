# Reads settings from configuration file in JSON format.
#
# @config Location of file that contains configuration in JSON format
#
read.AzureSMR.config <- function(config = getOption("AzureSMR.config")){
  z <- tryCatch(fromJSON(file(config)),
                error = function(e)e
  )
  # Error check the settings file for invalid JSON
  if(inherits(z, "error")) {
    msg <- sprintf("Your config file contains invalid json", config)
    msg <- paste(msg, z$message, sep = "\n\n")
    stop(msg, call. = FALSE)
  }
  z
}
