#' List Storage containers for Specified Storage Account.
#'
#' @inheritParams setAzureContext
#' @inheritParams azureAuthenticate
#' @inheritParams azureSAGetKey

#' @family Container functions
#'
#' @export
azureListStorageContainers <- function(azureActiveContext, storageAccount, storageKey,
                                  resourceGroup, subscriptionID, verbose = FALSE) {
  assert_that(is.azureActiveContext(azureActiveContext))
  azureCheckToken(azureActiveContext)
  azToken <- azureActiveContext$Token

  if (missing(subscriptionID)) subscriptionID <- azureActiveContext$subscriptionID
  if (missing(resourceGroup)) resourceGroup <- azureActiveContext$resourceGroup
  if (missing(storageAccount)) storageAccount <- azureActiveContext$storageAccount
  if (missing(storageKey)) storageKey <- azureActiveContext$storageKey
  if (!is_storage_key(storageKey)) 
    storageKey <- azureSAGetKey(azureActiveContext, resourceGroup = resourceGroup, storageAccount = storageAccount)

  assert_that(is_subscription_id(subscriptionID))
  assert_that(is_resource_group(resourceGroup))
  assert_that(is_storage_account(storageAccount))
  assert_that(is_storage_key(storageKey))

  verbosity <- set_verbosity(verbose)

  URL <- paste0("http://", storageAccount, ".blob.core.windows.net/?comp=list")
  #browser()
  xdate <- x_ms_date()
  SIG <- getSig(azureActiveContext, url = URL, verb = "GET", key = storageKey, storageAccount = storageAccount,
                CMD = "\ncomp:list", date = xdate)

  sharedKey <- paste0("SharedKey ", storageAccount, ":", SIG)

  r <- GET(URL, azure_storage_header(sharedKey, date = xdate), verbosity)
  stopWithAzureError(r)

  r <- content(r, "text", encoding = "UTF-8")
  y <- htmlParse(r)

  namesx  <- xpathApply(y, "//containers//container/name", xmlValue)
  if (length(namesx) == 0) {
    message("No containers found in Storage account")
    return(
      data.frame(
        name            = character(0),
        `Last-Modified` = character(0),
        Status          = character(0),
        State           = character(0),
        Etag            = character(0),
        stringsAsFactors = FALSE
      )
    )
  }

  azureActiveContext$storageAccount <- storageAccount
  azureActiveContext$resourceGroup  <- resourceGroup
  azureActiveContext$storageKey     <- storageKey

  data.frame(
    name            = xpathSApply(y, "//containers//container/name", xmlValue),
    `Last-Modified` = xpathSApply(y, "//containers//container/properties/last-modified", xmlValue),
    Status          = xpathSApply(y, "//containers//container/properties/leasestatus", xmlValue),
    State           = xpathSApply(y, "//containers//container/properties/leasestate", xmlValue),
    Etag            = xpathSApply(y, "//containers//container/properties/etag",  xmlValue),
    stringsAsFactors = FALSE,
    check.names = FALSE
  )

}


#' Create Storage containers in a specified Storage Account.
#'
#' @inheritParams setAzureContext
#' @inheritParams azureAuthenticate
#' @inheritParams azureSAGetKey

#' @family Container functions
#'
#' @export
azureCreateStorageContainer <- function(azureActiveContext, container, storageAccount,
                                   storageKey, resourceGroup, subscriptionID, verbose = FALSE) {
  assert_that(is.azureActiveContext(azureActiveContext))
  azureCheckToken(azureActiveContext)
  azToken <- azureActiveContext$Token

  if (missing(subscriptionID)) subscriptionID <- azureActiveContext$subscriptionID
  if (missing(resourceGroup)) resourceGroup <- azureActiveContext$resourceGroup
  if (missing(storageAccount)) storageAccount <- azureActiveContext$storageAccount
  if (missing(storageKey)) storageKey <- azureActiveContext$storageKey
  if (!is_storage_key(storageKey))
    storageKey <- azureSAGetKey(azureActiveContext, resourceGroup = resourceGroup, storageAccount = storageAccount)

  assert_that(is_subscription_id(subscriptionID))
  assert_that(is_resource_group(resourceGroup))
  assert_that(is_storage_account(storageAccount))
  assert_that(is_storage_key(storageKey))

  verbosity <- set_verbosity(verbose)


  URL <- paste0("https://", storageAccount, ".blob.core.windows.net//", container, "?restype=container")

  container <- container

  azureActiveContext$container <- container
  azureActiveContext$storageAccount <- storageAccount
  azureActiveContext$resourceGroup <- resourceGroup

  URL <- paste0("http://", storageAccount, ".blob.core.windows.net/", container, "?restype=container")
  xdate <- x_ms_date()
  SIG <- getSig(azureActiveContext, url = URL, verb = "PUT", key = storageKey,
                storageAccount = storageAccount, container = container,
                CMD = "\nrestype:container", date = xdate)

  sharedKey <- paste0("SharedKey ", storageAccount, ":", SIG)
  r <- PUT(URL, azure_storage_header(sharedKey, date = xdate), verbosity)

  if (status_code(r) == 201) {
    message("OK. Container created.")
    return(TRUE)
  }
  if (status_code(r) == 409) {
    message("OK. The specified container already exists.")
    return(TRUE)
  }

  stopWithAzureError(r)
  message("OK")
  return(TRUE)
}


#' Delete Storage container in a specified Storage Account.
#'
#' @inheritParams setAzureContext
#' @inheritParams azureAuthenticate
#' @inheritParams azureSAGetKey

#' @family Container functions
#'
#' @export
azureDeleteStorageContainer <- function(azureActiveContext, container, storageAccount,
                                   storageKey, resourceGroup, subscriptionID, verbose = FALSE) {
  if (missing(subscriptionID)) subscriptionID <- azureActiveContext$subscriptionID
  if (missing(resourceGroup)) resourceGroup <- azureActiveContext$resourceGroup
  if (missing(storageAccount)) storageAccount <- azureActiveContext$storageAccount
  if (missing(storageKey)) storageKey <- azureActiveContext$storageKey
  if (!is_storage_key(storageKey))
    storageKey <- azureSAGetKey(azureActiveContext, resourceGroup = resourceGroup, storageAccount = storageAccount)

  assert_that(is_subscription_id(subscriptionID))
  assert_that(is_resource_group(resourceGroup))
  assert_that(is_storage_account(storageAccount))
  assert_that(is_container(container))
  assert_that(is_storage_key(storageKey))

  azureCheckToken(azureActiveContext)


  verbosity <- set_verbosity(verbose)

  container <- container

  
  storageKey <- refreshStorageKey(azureActiveContext, storageAccount, resourceGroup)
  if (length(storageKey) < 1) {
    stop("Error: No storageKey provided: Use storageKey argument or set in AzureContext")
  }


  URL <- paste0("http://", storageAccount, ".blob.core.windows.net/", container, "?restype=container")
  xdate <- x_ms_date()
  SIG <- getSig(azureActiveContext, url = URL, verb = "DELETE", key = storageKey,
                storageAccount = storageAccount,
                CMD = paste0(container, "\nrestype:container"), date = xdate)

  sharedKey <- paste0("SharedKey ", storageAccount, ":", SIG)

  r <- DELETE(URL, azure_storage_header(sharedKey, date = xdate), verbosity)
  stopWithAzureError(r)

  azureActiveContext$container <- container
  azureActiveContext$storageAccount <- storageAccount
  azureActiveContext$resourceGroup <- resourceGroup

  if (status_code(r) == 202) message("container delete request accepted")
  return(TRUE)
}
