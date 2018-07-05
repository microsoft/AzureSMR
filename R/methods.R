
# azureActiveContext ----

#' azureActiveContext object.
#'
#' Functions for creating and displaying information about azureActiveContext objects.
#'
#' @param x the Object to create, test or print
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

# --- refreshToken

is_refreshToken <- function(x) {
  is.character(x) && length(x) == 1 && nchar(x) > 0
}

on_failure(is_refreshToken) <- function(call, env) {
  "Provide a valid refreshToken argument"
}

# --- deviceToken

is_deviceCode <- function(x) {
  is.character(x) && length(x) == 1 && nchar(x) > 0
}

on_failure(is_deviceCode) <- function(call, env) {
  "Provide a valid deviceCode argument"
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

# --- relativePath

is_relativePath <- function(x) {
  !missing(x) && !is.null(x) && is.character(x) && length(x) == 1
}

on_failure(is_relativePath) <- function(call, env) {
  "Provide a valid relativePath string"
}

# --- destinationRelativePath

is_destinationRelativePath <- function(x) {
  !missing(x) && !is.null(x) && is.character(x) && length(x) == 1
}

on_failure(is_destinationRelativePath) <- function(call, env) {
  "Provide a valid destinationRelativePath string"
}

# --- sourceRelativePaths

is_sourceRelativePaths <- function(x) {
  !missing(x) && !is.null(x) && is.character(x) && length(x) > 0
}

on_failure(is_sourceRelativePaths) <- function(call, env) {
  "Provide a non empty vector of sourceRelativePath paths"
}

# --- sourceRelativePath

is_sourceRelativePath <- function(x) {
  !missing(x) && !is.null(x) && is.character(x) && length(x) == 1
}

on_failure(is_sourceRelativePath) <- function(call, env) {
  "Provide a valid sourceRelativePath string"
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

# --- bufferSize

is_bufferSize <- function(x) {
  is.integer(x) && length(x) == 1 && x > 0
}

on_failure(is_bufferSize) <- function(call, env) {
  "Provide a valid integer bufferSize. e.g., 4194304L, 1048576L, 1024L, 128L"
}

# --- contentSize

is_contentSize <- function(x) {
  is.integer(x) && length(x) == 1 && x >= -1
}

on_failure(is_contentSize) <- function(call, env) {
  "Provide a valid integer contentSize. e.g., 4194304L, 1048576L, 1024L, 128L"
}

# --- replication

is_replication <- function(x) {
  is.integer(x) && length(x) == 1 && x > 0
}

on_failure(is_replication) <- function(call, env) {
  "Provide a valid integer replication. e.g., 1L, 3L, 5L"
}

# --- blockSize

is_blockSize <- function(x) {
  is.integer(x) && length(x) == 1 && x > 0
}

on_failure(is_blockSize) <- function(call, env) {
  "Provide a valid integer blockSize. e.g., 67108864L, 134217728L, 268435456L"
}

# --- offset

is_offset <- function(x) {
  is.integer(x) && x >= 0
}

on_failure(is_offset) <- function(call, env) {
  "Provide a valid integer offset that is >= 0. e.g., 4194304L, 67108864L"
}

# --- length

is_length <- function(x) {
  is.integer(x) && x >= 0
}

on_failure(is_length) <- function(call, env) {
  "Provide a valid integer length that is >=0. e.g., 4194304L, 134217728L"
}

# --- position (remote file cursor)

is_position <- function(x) {
  is.integer(x) && x >= 0
}

on_failure(is_position) <- function(call, env) {
  "Provide a valid integer position that is >=0. e.g., 4194304L, 134217728L"
}

# --- auth type

is_authType <- function(x) {
  is.character(x) && length(x) == 1 && nchar(x) > 0 && assert_that(is_valid_authType(x))
}

on_failure(is_authType) <- function(call, env) {
  "Provide a valid authType string"
}

is_valid_authType <- function(x) {
  x == "ClientCredential" || x == "DeviceCode" || x == "RefreshToken"
}

on_failure(is_valid_authType) <- function(call, env) {
  paste("authType string must be a string",
        "and should be one of \"ClientCredential\", \"DeviceCode\" or \"RefreshToken\".",
        sep = "\n")
}

# --- resource

is_resource <- function(x) {
  is.character(x) && length(x) == 1 && nchar(x) > 0 && assert_that(is_valid_resource(x))
}

on_failure(is_resource) <- function(call, env) {
  "Provide a valid resource string"
}

is_valid_resource <- function(x) {
  grepl("^(https?:\\/\\/)?([\\da-z\\.-]+)\\.([a-z\\.]{2,6})([\\/\\w \\.-]*)*\\/?$", x)
}

on_failure(is_valid_resource) <- function(call, env) {
  paste("resource must be a string",
        "and should be in a valid URL format.",
        sep = "\n")
}

# --- content

is_content <- function(x) {
  is.raw(x) && getContentSize(x) >= 0
}

on_failure(is_content) <- function(call, env) {
  "Provide a valid non-null raw content"
}

# adlsAccount ----

is_adls_account <- function(x) {
  is.character(x) && length(x) == 1 && assert_that(is_valid_adls_account(x))
}

on_failure(is_adls_account) <- function(call, env) {
  "Provide a valid adls account, or set using createAzureContext()"
}

is_valid_adls_account <- function(x) {
  nchar(x) >= 3 && nchar(x) <= 24 && grepl("^[a-z0-9-]*$", x)
}

on_failure(is_valid_storage_account) <- function(call, env) {
  paste("ADLS account name must be between 3 and 24 characters in length",
        "and use numbers and lower-case letters and '-' only.",
        sep = "\n")
}

# adlFileOutputStream ----

#' adlFileOutputStream object.
#'
#' Functions for creating and displaying information about adlFileOutputStream objects.
#'
#' @seealso [createAdlFileOutputStream()]
#' @export
#' @rdname Internal
as.adlFileOutputStream <- function(x){
  if(!is.environment(x)) stop("Expecting an environment as input")
  class(x) <- "adlFileOutputStream"
  x
}

#' @export
#' @rdname Internal
is.adlFileOutputStream <- function(x){
  inherits(x, "adlFileOutputStream")
}

#' @export
on_failure(is.adlFileOutputStream) <- function(call, env) {
  "Provide a valid adlFileOutputStream. See createAdlFileOutputStream()"
}

#' @export
print.adlFileOutputStream <- function(x, ...){
  cat("AzureSMR adlFileOutputStream\n")
  #cat("Tenant ID :", x$tenantID, "\n")
  #cat("Subscription ID :", x$subscriptionID, "\n")
}

#' @export
str.adlFileOutputStream <- function(object, ...){
  cat(("AzureSMR adlFileOutputStream with elements:\n"))
  ls.str(object, all.names = TRUE)
}

#' Check for proper adlFileOutputStream.
#'
#' @inheritParams createAdlFileOutputStream
#' @param adlFileOutputStream the adlFileOutputStream object to check
#' @family Azure resource functions
#' @export
adlFileOutputStreamCheck <- function(adlFileOutputStream) {
  if (missing(adlFileOutputStream) || is.null(adlFileOutputStream)) return(FALSE)
  if (adlFileOutputStream$streamClosed) {
    stop("IOException: Attempting to write to a closed stream")
  }
  return(TRUE)
}

# adlFileInputStream ----

#' adlFileInputStream object.
#'
#' Functions for creating and displaying information about adlFileInputStream objects.
#'
#' @seealso [createAdlFileInputStream()]
#' @export
#' @rdname Internal
as.adlFileInputStream <- function(x){
  if(!is.environment(x)) stop("Expecting an environment as input")
  class(x) <- "adlFileInputStream"
  x
}

#' @export
#' @rdname Internal
is.adlFileInputStream <- function(x){
  inherits(x, "adlFileInputStream")
}

#' @export
on_failure(is.adlFileInputStream) <- function(call, env) {
  "Provide a valid adlFileInputStream. See createAdlFileInputStream()"
}

#' @export
print.adlFileInputStream <- function(x, ...){
  cat("AzureSMR adlFileInputStream\n")
  #cat("Tenant ID :", x$tenantID, "\n")
  #cat("Subscription ID :", x$subscriptionID, "\n")
}

#' @export
str.adlFileInputStream <- function(object, ...){
  cat(("AzureSMR adlFileInputStream with elements:\n"))
  ls.str(object, all.names = TRUE)
}

#' Check for proper adlFileInputStream.
#'
#' @inheritParams createAdlFileInputStream
#' @param adlFileInputStream the adlFileInputStream object to check
#' @family Azure resource functions
#' @export
adlFileInputStreamCheck <- function(adlFileInputStream) {
  if (missing(adlFileInputStream) || is.null(adlFileInputStream)) return(FALSE)
  if (adlFileInputStream$streamClosed) {
    stop("IOException: Attempting to read from a closed stream")
  }
  return(TRUE)
}

# adlRetryPolicy ----

#' adlRetryPolicy object.
#'
#' Functions for creating and displaying information about adlRetryPolicy objects.
#'
#' @seealso [createAdlFileInputStream()]
#' @export
#' @rdname Internal
as.adlRetryPolicy <- function(x){
  if(!is.environment(x)) stop("Expecting an environment as input")
  class(x) <- "adlRetryPolicy"
  x
}

#' @export
#' @rdname Internal
is.adlRetryPolicy <- function(x){
  inherits(x, "adlRetryPolicy")
}

#' @export
on_failure(is.adlRetryPolicy) <- function(call, env) {
  "Provide a valid adlRetryPolicy. See createAdlRetryPolicy()"
}

#' @export
print.adlRetryPolicy <- function(x, ...){
  cat("AzureSMR adlRetryPolicy\n")
  cat("Retry count :", x$retryCount, "\n")
  cat("Max retries :", x$maxRetries, "\n")
  cat("Exponential retry interval :", x$exponentialRetryInterval, "\n")
  cat("Exponential factor :", x$exponentialFactor, "\n")
  cat("Last attempt start time :", x$lastAttemptStartTime, "\n")
}

#' @export
as.character.adlRetryPolicy <- function(x, ...) {
  xStr <- paste0("AzureSMR adlRetryPolicy:\n"
         , " Retry policy type: ", x$retryPolicyType, "\n"
         , " Max retries: ", x$maxRetries, "\n"
         , " Exponential retry interval: ", x$exponentialRetryInterval, "\n"
         , " Exponential factor: ", x$exponentialFactor, "\n"
  )
  if(x$retryPolicyType == retryPolicyEnum$EXPONENTIALBACKOFF) {
    xStr <- paste0(xStr
                   , " Retry count: ", x$retryCount, "\n")
  } else if(x$retryPolicyType == retryPolicyEnum$NONIDEMPOTENT) {
    xStr <- paste0(xStr
                   , " Retry count 401: ", x$retryCount401, "\n"
                   , " Retry count 429: ", x$retryCount429, "\n"
                   )
  }
  return(xStr)
}

#' @export
str.adlRetryPolicy <- function(object, ...){
  cat(("AzureSMR adlRetryPolicy with elements:\n"))
  ls.str(object, all.names = TRUE)
}
