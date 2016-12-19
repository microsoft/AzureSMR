AzureSMR.config.default <- "~/.azureml/settings.json"

.onAttach <- function(libname, pkgname){
  options(AzureSMR.config = AzureSMR.config.default)
}
