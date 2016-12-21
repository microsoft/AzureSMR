#' azureActiveContext object.
#'
#' Functions for creating and displaying information about azureActiveContext objects.
#'
#' @param x Object to create, test or print
#' @param ... Ignored
#'
#' @seealso \code{\link{createAzureContext}}
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
#' @rdname Internal
print.azureActiveContext <- function(x, ...){
  cat("AzureSMR azureActiveContext\n")
  cat("Tenant ID :", x$tenantID, "\n")
}

#' @export
#' @rdname Internal
str.azureActiveContext <- function(x, ...){
  cat(("AzureSMR azureActiveContext with elements:\n"))
  ls.str(x, all.names = TRUE)
}
