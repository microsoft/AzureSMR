#' List batch accounts.
#'
#' @inheritParams setAzureContext
#' @inheritParams azureAuthenticate
#' @inheritParams azureBatchGetKey

#' @family Batch account functions
#' @export
azureListBatchAccounts <- function(azureActiveContext, resourceGroup, subscriptionID,
                        verbose = FALSE) {
  assert_that(is.azureActiveContext(azureActiveContext))
  azureCheckToken(azureActiveContext)
  azToken <- azureActiveContext$Token
  
  if (missing(subscriptionID)) subscriptionID <- azureActiveContext$subscriptionID
  assert_that(is_subscription_id(subscriptionID))
  verbosity <- set_verbosity(verbose) 
  
  type_batch <- "Microsoft.Batch/batchAccounts"
  
  z <- if(missing(resourceGroup)) {
    azureListAllResources(azureActiveContext, type = type_batch)
  } else {
    azureListAllResources(azureActiveContext, type = type_batch, resourceGroup = resourceGroup, subscriptionID = subscriptionID)
  }
  rownames(z) <- NULL
  z$batchAccount <- extractStorageAccount(z$id)
  z
}

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
  
  body <- paste0('{
                  "location":"', location, '",
                  }'
  )  
  
  uri <- paste0("https://management.azure.com/subscriptions/", subscriptionID,
                "/resourceGroups/", resourceGroup, "/providers/Microsoft.Batch/batchAccounts/",
                batchAccount, "?api-version=2017-05-01")
  
  r <- call_azure_sm(azureActiveContext, uri = uri, body = body,
                     verb = "PUT", verbose = verbose)
  
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
      batchAccount %in% azureListBatchAccounts(azureActiveContext, subscriptionID = subscriptionID)$name
    )
  }
  TRUE
}

#' Delete an Azure Batch Account.
#'
#' @inheritParams setAzureContext
#' @inheritParams azureAuthenticate
#' @inheritParams azureBatchGetKey

#' @family Batch account functions
#' @export
azureDeleteBatchAccount <- function(azureActiveContext, batchAccount,
                                      resourceGroup, subscriptionID, verbose = FALSE) {
  assert_that(is.azureActiveContext(azureActiveContext))
  azureCheckToken(azureActiveContext)
  azToken <- azureActiveContext$Token
  
  if (missing(resourceGroup)) resourceGroup <- azureActiveContext$resourceGroup
  if (missing(subscriptionID)) subscriptionID <- azureActiveContext$subscriptionID
  
  assert_that(is_storage_account(batchAccount))
  assert_that(is_resource_group(resourceGroup))
  assert_that(is_subscription_id(subscriptionID))
  
  uri <- paste0("https://management.azure.com/subscriptions/", subscriptionID,
                "/resourceGroups/", resourceGroup, "/providers/Microsoft.Batch/batchAccounts/",
                batchAccount, "?api-version=2017-05-01")
  
  r <- call_azure_sm(azureActiveContext, uri = uri,
                     verb = "DELETE", verbose = verbose)
  
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
  
  assert_that(is_storage_account(batchAccount))
  assert_that(is_resource_group(resourceGroup))
  assert_that(is_subscription_id(subscriptionID))

  message("Fetching Batch Key..")
  
  uri <- paste0("https://management.azure.com/subscriptions/", subscriptionID,
                "/resourceGroups/", resourceGroup, 
                "/providers/Microsoft.Batch/batchAccounts/", batchAccount, 
                "/listkeys?api-version=2017-05-01")
  
  r <- call_azure_sm(azureActiveContext, uri = uri,
                     verb = "POST", verbose = verbose)
  stopWithAzureError(r)
  
  rl <- content(r, "text", encoding = "UTF-8")
  df <- fromJSON(rl)
  azureActiveContext$batchAccount  <- batchAccount
  azureActiveContext$resourceGroup   <- resourceGroup
  azureActiveContext$batchKey <- df$primary
  
  return(azureActiveContext$batchKey)
}