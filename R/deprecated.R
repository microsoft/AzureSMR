#' Dumps out the contents of the AzureContext.
#'
#' @inheritParams setAzureContext
#' @rdname pkg-deprecated
#' @export
dumpAzureContext <- function(azureActiveContext) {
  .Deprecated("str")
  str(azureActiveContext)
}


#' Deprecated functions.
#'
#' @rdname pkg-deprecated
#' @param ... passed to [azureListRG()]
#' @export
AzureListRG <- function(...) {
  .Deprecated("azureListRG")
  azureListRG(...)
}
