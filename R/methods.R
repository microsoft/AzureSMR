#' azureActiveContext object.
#'
#' Functions for creating and displaying information about azureActiveContext objects.
#'
#' @param x Object to create, test or print
#' @param ... Ignored
#'
#' @seealso [createAzureContext()]
#' @export
#' @rdname Internal
as.azureActiveContext <- function(x){
  if(!is.environment(x)) stop("Expecting an environment as input")
  class(x) <- "azureActiveContext"
  x
}

#' @export
#' @rdname Internal
is.azureActiveContext <- function(x){
  inherits(x, "azureActiveContext")
}

#' @export
print.azureActiveContext <- function(x, ...){
  cat("AzureSMR azureActiveContext\n")
  cat("Tenant ID :", x$tenantID, "\n")
  cat("Subscription ID :", x$subscriptionID, "\n")
  cat("Resource group  :", x$resourceGroup, "\n")
  cat("Storage account :", x$storageAccount, "\n")
}

#' @export
#' @importFrom utils str
str.azureActiveContext <- function(object, ...){
  cat(("AzureSMR azureActiveContext with elements:\n"))
  ls.str(object, all.names = TRUE)
}
