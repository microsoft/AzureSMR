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
str.azureActiveContext <- function(object, ...){
  cat(("AzureSMR azureActiveContext with elements:\n"))
  ls.str(object, all.names = TRUE)
}

on_failure(is.azureActiveContext) <- function(call, env) {
  paste0(deparse(call$x), " is not a valid azureActiveContext. See createAzureContext()")
}

#--------------------------------------------------------------------------

#' @importFrom assertthat assert_that on_failure<-
is_resource_group <- function(x) {
  is.character(x) && length(x) == 1 && nchar(x) > 0
}

on_failure(is_resource_group) <- function(call, env) {
  paste0(deparse(call$x), "Provide a valid resourceGroup argument, or set using createAzureContext()")
}


# --- subscription ID

is_subscription_id <- function(x) {
  is.character(x) && length(x) == 1 && nchar(x) > 0
}

on_failure(is_subscription_id) <- function(call, env) {
  paste0(deparse(call$x), "Provide a valid subscriptionID argument, or set using createAzureContext()")
}

# --- location

is_location <- function(x) {
  is.character(x) && length(x) == 1 && nchar(x) > 0
}

on_failure(is_location) <- function(call, env) {
  paste0(deparse(call$x), "Provide a valid location (Azure region, e.g. 'South Central US')")
}


# --- tenant ID

is_tenant_id <- function(x) {
  is.character(x) && length(x) == 1 && nchar(x) > 0
}

on_failure(is_tenant_id) <- function(call, env) {
  paste0(deparse(call$x), "Provide a valid tenantID argument, or set using createAzureContext()")
}

# --- client ID

is_client_id <- function(x) {
  is.character(x) && length(x) == 1 && nchar(x) > 0
}

on_failure(is_client_id) <- function(call, env) {
  paste0(deparse(call$x), "Provide a valid clientID argument, or set using createAzureContext()")
}

# --- authKey

is_authKey <- function(x) {
  is.character(x) && length(x) == 1 && nchar(x) > 0
}

on_failure(is_authKey) <- function(call, env) {
  paste0(deparse(call$x), "Provide a valid autkKeyID argument, or set using createAzureContext()")
}

# --- vm_name

is_vm_name <- function(x) {
  is.character(x) && length(x) == 1 && nchar(x) > 0
}

on_failure(is_vm_name) <- function(call, env) {
  paste0(deparse(call$x), "Provide a valid vm_name (Azure region, e.g. 'South Central US')")
}

# --- storage_account

is_storage_account <- function(x) {
  is.character(x) && length(x) == 1 && nchar(x) > 0
}

on_failure(is_storage_account) <- function(call, env) {
  paste0(deparse(call$x), "Provide a valid storageAccount, or set using createAzureContext()")
}

# --- container

is_container <- function(x) {
  is.character(x) && length(x) >= 1
}

on_failure(is_container) <- function(call, env) {
  paste0(deparse(call$x), "Provide a valid container, or set using createAzureContext()")
}
7
# --- storage_key

is_storage_key <- function(x) {
  is.character(x) && length(x) == 1 && nchar(x) > 0
}

on_failure(is_storage_key) <- function(call, env) {
  paste0(deparse(call$x), "Provide a valid storageKey, or set using createAzureContext()")
}

# --- blob

is_blob <- function(x) {
  is.character(x) && length(x) == 1 && nchar(x) > 0
}

on_failure(is_blob) <- function(call, env) {
  paste0(deparse(call$x), "Provide a valid blob, or set using createAzureContext()")
}

# --- deployment name

is_deployment_name <- function(x) {
  is.character(x) && length(x) == 1 && nchar(x) > 0
}

on_failure(is_deployment_name) <- function(call, env) {
  paste0(deparse(call$x), "Provide a deplname")
}

# --- scaleset

is_scaleset <- function(x) {
  is.character(x) && length(x) == 1 && nchar(x) > 0
}

on_failure(is_scaleset) <- function(call, env) {
  paste0(deparse(call$x), "Provide a scaleset")
}
