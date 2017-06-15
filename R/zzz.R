AzureSMR.config.default <- ifelse(Sys.info()["sysname"] == "Windows", 
                                  paste0("C:/Users/", Sys.getenv("USERNAME"), "/.azuresmr/config.json"),
                                  "~/.azuresmr/config.json")

.onAttach <- function(libname, pkgname) {
  if (is.null(getOption("AzureSMR.config")))
  options(AzureSMR.config = AzureSMR.config.default)
}
