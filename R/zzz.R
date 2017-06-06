AzureSMR.config.default <- "~/.azuresmr/config.json"

.onAttach <- function(libname, pkgname) {
  if (is.null(getOption("AzureSMR.config")))
  options(AzureSMR.config = AzureSMR.config.default)
}
