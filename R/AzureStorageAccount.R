
#' List storage accounts.
#'
#' @inheritParams setAzureContext
#' @inheritParams azureAuthenticate
#' @inheritParams azureSAGetKey

#' @family Storage account functions
#' @export
azureListSA <- function(azureActiveContext, resourceGroup, subscriptionID,
                        verbose = FALSE) {
  assert_that(is.azureActiveContext(azureActiveContext))
  azureCheckToken(azureActiveContext)
  azToken <- azureActiveContext$Token

  if (missing(subscriptionID)) subscriptionID <- azureActiveContext$subscriptionID
  assert_that(is_subscription_id(subscriptionID))
  verbosity <- set_verbosity(verbose) 

  type_sa <- "Microsoft.Storage/storageAccounts"

  z <- if(missing(resourceGroup)) {
    azureListAllResources(azureActiveContext, type = type_sa)
  } else {
    azureListAllResources(azureActiveContext, type = type_sa, resourceGroup = resourceGroup)
  }
  rownames(z) <- NULL
  z$storageAccount <- extractStorageAccount(z$id)
  z
}

#' Get specified storage account
#'
#' @inheritParams setAzureContext
#' @inheritParams azureAuthenticate
#' @inheritParams azureSAGetKey

#' @family Storage account functions
#' @export
azureGetStorageAccount <- function(azureActiveContext,
                                   resourceGroup,
                                   subscriptionID,
                                   storageAccount,
                                   verbose = FALSE) {
  if (missing(resourceGroup)) {
    resourceGroup <- azureActiveContext$resourceGroup
  } 
  
  if (missing(subscriptionID)) {
    subscriptionID <- azureActiveContext$subscriptionID
  } 
  if (!length(subscriptionID)) {
    stop("Error: No subscriptionID provided: Use SUBID argument or set in AzureContext")
  }
  
  verbosity <- if (verbose) httr::verbose(TRUE) else NULL
  
  SA <- if(missing(resourceGroup)) {
    azureListAllResources(azureActiveContext,
                          type = "Microsoft.Storage/storageAccounts")
  } else {
    azureListAllResources(azureActiveContext,
                          type = "Microsoft.Storage/storageAccounts",
                          resourceGroup = resourceGroup)
    
  }
  
  idx <- which(SA$name == storageAccount)
  if (identical(idx, integer(0))) {
    stop(sprintf("Storage account '%s' was not found in subscription '%s'.",
                 storageAccount,
                 subscriptionID))
  }
  
  list(
    name = SA$name[idx],
    type = SA$type[idx],
    location = SA$location[idx],
    id = SA$id[idx],
    resourceGroup = SA$resourceGroup[idx],
    subscriptionID = SA$subscriptionID[idx]
  )
}

#' Get the Storage Keys for Specified Storage Account.
#'
#' @inheritParams setAzureContext
#' @inheritParams azureAuthenticate
#'
#' @family Storage account functions
#' @export
azureSAGetKey <- function(azureActiveContext, storageAccount, 
                          resourceGroup, subscriptionID, verbose = FALSE) {
  assert_that(is.azureActiveContext(azureActiveContext))
  azureCheckToken(azureActiveContext)
  azToken <- azureActiveContext$Token

  if (missing(resourceGroup)) resourceGroup <- azureActiveContext$resourceGroup
  if (missing(subscriptionID)) subscriptionID <- azureActiveContext$subscriptionID

  assert_that(is_storage_account(storageAccount))
  assert_that(is_resource_group(resourceGroup))
  assert_that(is_subscription_id(subscriptionID))
  verbosity <- set_verbosity(verbose)

  message("Fetching Storage Key..")

  URL <- paste0("https://management.azure.com/subscriptions/", subscriptionID,
               "/resourceGroups/", resourceGroup, 
               "/providers/Microsoft.Storage/storageAccounts/", storageAccount, 
               "/listkeys?api-version=2016-01-01")

  r <- POST(URL, azureApiHeaders(azToken), verbosity)
  stopWithAzureError(r)

  rl <- content(r, "text", encoding = "UTF-8")
  df <- fromJSON(rl)
  azureActiveContext$storageAccount  <- storageAccount
  azureActiveContext$resourceGroup   <- resourceGroup
  azureActiveContext$storageKey <- df$keys$value[1]

  return(azureActiveContext$storageKey)
}



#' Create an Azure Storage Account.
#'
#' @inheritParams setAzureContext
#' @inheritParams azureAuthenticate
#' @inheritParams azureSAGetKey
#' @param location A string for the location to create storage account
#' @param asynchronous If TRUE, submits asynchronous request to Azure. Otherwise waits until storage account is created.
#' @family Storage account functions
#' @export
azureCreateStorageAccount <- function(azureActiveContext, storageAccount,
                                      location = "northeurope",
                                      resourceGroup, subscriptionID, 
                                      asynchronous = FALSE, verbose = FALSE) {
  assert_that(is.azureActiveContext(azureActiveContext))
  azureCheckToken(azureActiveContext)
  azToken <- azureActiveContext$Token

  if (missing(subscriptionID)) subscriptionID <- azureActiveContext$subscriptionID
  if (missing(resourceGroup)) resourceGroup <- azureActiveContext$resourceGroup
  assert_that(is_resource_group(resourceGroup))
  assert_that(is_subscription_id(subscriptionID))
  assert_that(is_storage_account(storageAccount))

  verbosity <- set_verbosity(verbose)

  bodyI <- paste0('{
  "location":"', location, '",
  "sku": {
    "name": "Standard_LRS"
  }}'
  )  
  
  URL <- paste0("https://management.azure.com/subscriptions/", subscriptionID,
               "/resourceGroups/", resourceGroup, "/providers/Microsoft.Storage/storageAccounts/",
               storageAccount, "?api-version=2016-01-01")

  r <- PUT(URL, azureApiHeaders(azToken), body = bodyI, encode = "json", verbosity)

  if (status_code(r) == 409) {
    message("409: Conflict : Account already exists with the same name")
    return(TRUE)
  }

  if (status_code(r) == 200) {
    message("Account already exists with the same properties")
  }
  stopWithAzureError(r)

  rl <- content(r, "text", encoding = "UTF-8")
  azureActiveContext$storageAccount <- storageAccount
  azureActiveContext$resourceGroup  <- resourceGroup
  message("Create request accepted")
  message("It can take a few moments to provision the storage account")

  if (!asynchronous) {
    wait_for_azure(
      storageAccount %in% azureListSA(azureActiveContext)$storageAccount
      )
  }
  TRUE
}


#' Delete an Azure Storage Account.
#'
#' @inheritParams setAzureContext
#' @inheritParams azureAuthenticate
#' @inheritParams azureSAGetKey

#' @family Storage account functions
#' @export
azureDeletestorageAccount <- function(azureActiveContext, storageAccount,
                                      resourceGroup, subscriptionID, verbose = FALSE) {
  assert_that(is.azureActiveContext(azureActiveContext))
  azureCheckToken(azureActiveContext)
  azToken <- azureActiveContext$Token

  if (missing(resourceGroup)) resourceGroup <- azureActiveContext$resourceGroup
  if (missing(subscriptionID)) subscriptionID <- azureActiveContext$subscriptionID

  assert_that(is_storage_account(storageAccount))
  assert_that(is_resource_group(resourceGroup))
  assert_that(is_subscription_id(subscriptionID))
  verbosity <- set_verbosity(verbose) 

  URL <- paste0("https://management.azure.com/subscriptions/", subscriptionID,
               "/resourceGroups/", resourceGroup, "/providers/Microsoft.Storage/storageAccounts/",
               storageAccount, "?api-version=2016-01-01")

  r <- DELETE(URL, azureApiHeaders(azToken), verbosity)

  if (status_code(r) == 204) {
    warning("Storage Account not found")
    return(FALSE)
  }
  if (status_code(r) != 200) stopWithAzureError(r)

  azureActiveContext$storageAccount <- storageAccount
  azureActiveContext$resourceGroup <- resourceGroup
  TRUE
}

