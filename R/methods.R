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
  "Provide a valid azureActiveContext. See createAzureContext()"
}

#--------------------------------------------------------------------------

#' @importFrom assertthat assert_that on_failure<-
is_resource_group <- function(x) {
  is.character(x) && length(x) == 1 && nchar(x) > 0
}

on_failure(is_resource_group) <- function(call, env) {
  "Provide a valid resourceGroup argument, or set using createAzureContext()"
}


# --- subscription ID

is_subscription_id <- function(x) {
  is.character(x) && length(x) == 1 && nchar(x) > 0
}

on_failure(is_subscription_id) <- function(call, env) {
  "Provide a valid subscriptionID argument, or set using createAzureContext()"
}

# --- location

is_location <- function(x) {
  is.character(x) && length(x) == 1 && nchar(x) > 0
}

on_failure(is_location) <- function(call, env) {
  "Provide a valid location (Azure region, e.g. 'South Central US')"
}


# --- tenant ID

is_tenant_id <- function(x) {
  is.character(x) && length(x) == 1 && nchar(x) > 0
}

on_failure(is_tenant_id) <- function(call, env) {
  "Provide a valid tenantID argument, or set using createAzureContext()"
}

# --- client ID

is_client_id <- function(x) {
  is.character(x) && length(x) == 1 && nchar(x) > 0
}

on_failure(is_client_id) <- function(call, env) {
  "Provide a valid clientID argument, or set using createAzureContext()"
}

# --- authKey

is_authKey <- function(x) {
  is.character(x) && length(x) == 1 && nchar(x) > 0
}

on_failure(is_authKey) <- function(call, env) {
  "Provide a valid autkKeyID argument, or set using createAzureContext()"
}

# --- vm_name

is_vm_name <- function(x) {
  is.character(x) && length(x) == 1 && nchar(x) > 0
}

on_failure(is_vm_name) <- function(call, env) {
  "Provide a valid vm_name (Azure region, e.g. 'South Central US')"
}

# --- storage_account

is_storage_account <- function(x) {
  is.character(x) && length(x) == 1 && assert_that(is_valid_storage_account(x))
}

on_failure(is_storage_account) <- function(call, env) {
  "Provide a valid storageAccount, or set using createAzureContext()"
}

is_valid_storage_account <- function(x) {
  nchar(x) >= 3 && nchar(x) <= 24 && grepl("^[a-z0-9]*$", x)
}

on_failure(is_valid_storage_account) <- function(call, env) {
    paste("Storage account name must be between 3 and 24 characters in length",
        "and use numbers and lower-case letters only.",
        sep = "\n")
}

# --- container

is_container <- function(x) {
  is.character(x) && length(x) == 1 && nchar(x) > 0
}

on_failure(is_container) <- function(call, env) {
  "Provide a valid container, or set using createAzureContext()"
}

# --- directory

is_directory <- function(x) {
  is.character(x) && length(x) == 1
}

on_failure(is_directory) <- function(call, env) {
  "Provide a valid directory, or set using createAzureContext()"
}

# --- storage_key

is_storage_key <- function(x) {
  is.character(x) && length(x) == 1 && nchar(x) > 0
}

on_failure(is_storage_key) <- function(call, env) {
  "Provide a valid storageKey, or set using createAzureContext()"
}

# --- blob

is_blob <- function(x) {
  is.character(x) && length(x) == 1 && nchar(x) > 0
}

on_failure(is_blob) <- function(call, env) {
  "Provide a valid blob, or set using createAzureContext()"
}

# --- deployment name

is_deployment_name <- function(x) {
  is.character(x) && length(x) == 1 && nchar(x) > 0
}

on_failure(is_deployment_name) <- function(call, env) {
  "Provide a deplname"
}

# --- scaleset

is_scaleset <- function(x) {
  is.character(x) && length(x) == 1 && nchar(x) > 0
}

on_failure(is_scaleset) <- function(call, env) {
  "Provide a scaleset"
}

# --- clustername

is_clustername <- function(x) {
  !missing(x) && is.character(x) && length(x) == 1 && nchar(x) > 0
}

on_failure(is_clustername) <- function(call, env) {
  "Provide a clustername"
}

# --- admin user

is_admin_user <- function(x) {
  is.character(x) && length(x) == 1 && nchar(x) > 0
}

on_failure(is_admin_user) <- function(call, env) {
  "Provide an adminUser"
}

# --- admin password

is_valid_admin_password <- function(x) {
  nchar(x) >= 6 && 
  grepl("[A-Z]", x) && 
  grepl("[a-z]", x) && 
  grepl("[0-9]", x)
}

on_failure(is_valid_admin_password) <- function(call, env) {
  paste("The admin password must be greater than 6 characters and contain",
   "at least one uppercase char, one lowercase char and one digit", 
   sep = "\n")
}

is_admin_password <- function(x) {
  is.character(x) && length(x) == 1 && 
  assert_that(is_valid_admin_password(x))
}

on_failure(is_admin_password) <- function(call, env) {
  "Provide an adminPassword"
}

# --- ssh user

is_ssh_user <- function(x) {
  is.character(x) && length(x) == 1 && nchar(x) > 0
}

on_failure(is_ssh_user) <- function(call, env) {
  "Provide an sshUser"
}

# --- ssh password

is_valid_ssh_password <- function(x) {
  nchar(x) >= 6 && grepl("[A-Z]", x) && grepl("[a-z]", x) && grepl("0-9", x)
}

on_failure(is_valid_ssh_password) <- function(call, env) {
  paste("The ssh password must be greater than 6 characters and contain", 
  "at least one uppercase char, one lowercase char and one digit", 
  sep = "\n")
}

is_ssh_password <- function(x) {
  is.character(x) && length(x) == 1 && nchar(x) > 0
}

on_failure(is_ssh_password) <- function(call, env) {
  "Provide an sshPassword"
}

# --- permission

is_permission <- function(x) {
  is.character(x) && length(x) == 1 && assert_that(is_valid_permission(x))
}

on_failure(is_permission) <- function(call, env) {
  "Provide a valid octal permission string"
}

is_valid_permission <- function(x) {
  nchar(x) == 3 && grepl("^[0-7]*$", x)
}

on_failure(is_valid_permission) <- function(call, env) {
  paste("Permission string must be 3 in length",
        "and use numbers between 0 to 7 only.",
        sep = "\n")
}
