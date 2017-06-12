
#' Create an Azure Batch Account.
#'
#' @inheritParams setAzureContext
#' @inheritParams azureAuthenticate
#' @inheritParams azureBatchGetKey
#' @param location A string for the location to create batch account
#' @param asynchronous If TRUE, submits asynchronous request to Azure. Otherwise waits until batch account is created.
#' @family Batch account functions
#' @export
azureCreateBatchAccount <- function(azureActiveContext, batchAccount,
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
  assert_that(is_storage_account(batchAccount))
  
  verbosity <- set_verbosity(verbose)
  
  bodyI <- paste0('{
                  "location":"', location, '",
                  }'
  )  
  
  URL <- paste0("https://management.azure.com/subscriptions/", subscriptionID,
                "/resourceGroups/", resourceGroup, "/providers/Microsoft.Batch/batchAccounts/",
                batchAccount, "?api-version=2017-05-01")
  
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
  azureActiveContext$batchAccount <- batchAccount
  azureActiveContext$resourceGroup  <- resourceGroup
  message("Create request Accepted. It can take a few moments to provision the batch account")
  
  if (!asynchronous) {
    wait_for_azure(
      batchAccount %in% azureListSA(azureActiveContext)$batchAccount
    )
  }
  TRUE
}

#' Delete an Azure Batch Account.
#'
#' @inheritParams setAzureContext
#' @inheritParams azureAuthenticate
#' @inheritParams azureSAGetKey

#' @family Batch account functions
#' @export
azureDeleteBatchAccount <- function(azureActiveContext, batchAccount,
                                      resourceGroup, subscriptionID, verbose = FALSE) {
  assert_that(is.azureActiveContext(azureActiveContext))
  azureCheckToken(azureActiveContext)
  azToken <- azureActiveContext$Token
  
  if (missing(resourceGroup)) resourceGroup <- azureActiveContext$resourceGroup
  if (missing(subscriptionID)) subscriptionID <- azureActiveContext$subscriptionID
  
  assert_that(is_storage_account(batchKey))
  assert_that(is_resource_group(resourceGroup))
  assert_that(is_subscription_id(subscriptionID))
  verbosity <- set_verbosity(verbose) 
  
  URL <- paste0("https://management.azure.com/subscriptions/", subscriptionID,
                "/resourceGroups/", resourceGroup, "/providers/Microsoft.Batch/batchAccounts/",
                batchAccount, "?api-version=2017-05-01")
  
  r <- DELETE(URL, azureApiHeaders(azToken), verbosity)
  
  if (status_code(r) == 204) {
    warning("Batch Account not found")
    return(FALSE)
  }
  if (status_code(r) != 200) stopWithAzureError(r)
  
  azureActiveContext$batchAccount <- batchAccount
  azureActiveContext$resourceGroup <- resourceGroup
  TRUE
}

#' Get the Batch Keys for Specified Batch Account.
#'
#' @inheritParams setAzureContext
#' @inheritParams azureAuthenticate
#'
#' @family Batch account functions
#' @export
azureBatchGetKey <- function(azureActiveContext, batchAccount, 
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
                "/providers/Microsoft.Batch/batchAccounts/", batchAccount, 
                "/listkeys?api-version=2017-05-01")
  
  r <- POST(URL, azureApiHeaders(azToken), verbosity)
  stopWithAzureError(r)
  
  rl <- content(r, "text", encoding = "UTF-8")
  df <- fromJSON(rl)
  azureActiveContext$batchAccount  <- batchAccount
  azureActiveContext$resourceGroup   <- resourceGroup
  azureActiveContext$batchKey <- df$keys$value[1]
  
  return(azureActiveContext$batchKey)
}