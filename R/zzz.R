AzureSM.config.default <- "~/.azureml/settings.json"

.onAttach <- function(libname, pkgname){
  options(AzureML.config = AzureSM.config.default)
}
