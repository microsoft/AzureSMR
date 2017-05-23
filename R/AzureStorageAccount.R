
#' List storage accounts.
#'
#' @inheritParams setAzureContext
#' @inheritParams azureAuthenticate
#' @inheritParams azureSAGetKey

#' @family Storage account functions
#' @export
azureListSA <- function(azureActiveContext, resourceGroup, subscriptionID,
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
  verbosity <- set_verbosity(verbose) 

  SA <- if(missing(resourceGroup)) {
    azureListAllResources(azureActiveContext,
                          type = "Microsoft.Storage/storageAccounts")
  } else {
    azureListAllResources(azureActiveContext,
                          type = "Microsoft.Storage/storageAccounts",
                          resourceGroup = resourceGroup)

  }

  rownames(SA) <- NULL
  SA$storageAccount <- extractStorageAccount(SA$id)
  SA
}


#' Get the Storage Keys for Specified Storage Account.
#'
#' @inheritParams setAzureContext
#' @inheritParams azureAuthenticate
#' @param storageAccount storageAccount
#'
#' @family Storage account functions
#' @export
azureSAGetKey <- function(azureActiveContext, storageAccount, 
                          resourceGroup, subscriptionID, verbose = FALSE) {
  azureCheckToken(azureActiveContext)

  if (missing(resourceGroup)) {
    resourceGroup <- azureActiveContext$resourceGroup
  } 

  if (missing(subscriptionID)) {
    subscriptionID <- azureActiveContext$subscriptionID
  }

  if (!length(storageAccount)) {
    stop("Error: No Valid storageAccount provided")
  }
  if (length(resourceGroup) < 1) {
    stop("Error: No resourceGroup provided: Use resourceGroup argument or set in AzureContext")
  }
  if (!length(subscriptionID)) {
    stop("Error: No subscriptionID provided: Use SUBID argument or set in AzureContext")
  }
  verbosity <- set_verbosity(verbose)


  message("Fetching Storage Key..")

  URL <- paste("https://management.azure.com/subscriptions/", subscriptionID,
               "/resourceGroups/", resourceGroup, "/providers/Microsoft.Storage/storageAccounts/",
               storageAccount, "/listkeys?api-version=2016-01-01", sep = "")

  r <- POST(URL, add_headers(.headers = c(Host = "management.azure.com",
                                          Authorization = azureActiveContext$Token,
                                          `Content-type` = "application/json")),
            verbosity)


  rl <- content(r, "text", encoding = "UTF-8")
  df <- fromJSON(rl)
  azureActiveContext$storageAccount  <- storageAccount
  azureActiveContext$storageAccountK <- storageAccount
  azureActiveContext$resourceGroup   <- resourceGroup
  azureActiveContext$storageKey      <- df$keys$value[1]
  azureActiveContext$storageKey
}



#' Create an Azure Storage Account.
#'
#' @inheritParams setAzureContext
#' @inheritParams azureAuthenticate
#' @inheritParams azureSAGetKey
#' @param location A string for the location to create storage account
#' @family Storage account functions
#' @export
azureCreateStorageAccount <- function(azureActiveContext, storageAccount,
                                      location = "northeurope",
                                      resourceGroup, subscriptionID, verbose = FALSE) {
  azureCheckToken(azureActiveContext)

  if (missing(resourceGroup)) {
    resourceGroup <- azureActiveContext$resourceGroup
  } 

  if (missing(subscriptionID)) {
    subscriptionID <- azureActiveContext$subscriptionID
  } 
  if (!length(storageAccount)) {
    stop("Error: No Valid storageAccount provided")
  }
  if (length(resourceGroup) < 1) {
    stop("Error: No resourceGroup provided: Use resourceGroup argument or set in AzureContext")
  }
  if (!length(subscriptionID)) {
    stop("Error: No subscriptionID provided: Use SUBID argument or set in AzureContext")
  }
  verbosity <- set_verbosity(verbose)


  # https://management.azure.com/subscriptions/{subscriptionId}/resourceGroups/{resourceGroupname}/providers/Microsoft.Storage/storageAccounts/{accountname}?api-version={api-version}

  #bodyI = "
  #{
  #\"location\": \"northeurope\",
  #\"tags\": {
  #\"key1\": \"value1\"
  #},
  #\"properties\": {
  #\"accessTier\": \"Hot\"
  #},
  #\"sku\": {
  #\"name\": \"Standard_LRS\"
  #},
  #\"kind\": \"Storage\"
  #}"

  bodyI <- "{
  \"location\": \"llllllll\",
  \"sku\": {
  \"name\": \"Standard_LRS\"
  }}"
  
  
  bodyI <- gsub("llllllll", location, bodyI)
  
  URL <- paste("https://management.azure.com/subscriptions/", subscriptionID,
               "/resourceGroups/", resourceGroup, "/providers/Microsoft.Storage/storageAccounts/",
               storageAccount, "?api-version=2016-01-01", sep = "")

  r <- PUT(URL, add_headers(.headers = c(Host = "management.azure.com",
                                         Authorization = azureActiveContext$Token,
                                         `Content-type` = "application/json")),
           body = bodyI,
           encode = "json", verbosity)

  if (status_code(r) == 409) {
    warning("409: Conflict : Account already exists with the same name")
    return(FALSE)
  }


  if (status_code(r) == 200) {
    message("Account already exists with the same properties")
  } else if (status_code(r) != 202) {
    stopWithAzureError(r)
  }

  rl <- content(r, "text", encoding = "UTF-8")
  azureActiveContext$storageAccount <- storageAccount
  azureActiveContext$resourceGroup  <- resourceGroup
  message("Create request Accepted. It can take a few moments to provision the storage account")
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
  azureCheckToken(azureActiveContext)

  if (missing(resourceGroup)) {
    resourceGroup <- azureActiveContext$resourceGroup
  } 

  if (missing(subscriptionID)) {
    subscriptionID <- azureActiveContext$subscriptionID
  } 
  if (!length(storageAccount)) {
    stop("Error: No Valid storageAccount provided")
  }
  if (length(resourceGroup) < 1) {
    stop("Error: No resourceGroup provided: Use resourceGroup argument or set in AzureContext")
  }
  if (!length(subscriptionID)) {
    stop("Error: No subscriptionID provided: Use SUBID argument or set in AzureContext")
  }
  verbosity <- set_verbosity(verbose) 


  URL <- paste0("https://management.azure.com/subscriptions/", subscriptionID,
               "/resourceGroups/", resourceGroup, "/providers/Microsoft.Storage/storageAccounts/",
               storageAccount, "?api-version=2016-01-01")

  r <- DELETE(URL, add_headers(.headers = c(Host = "management.azure.com",
                                            Authorization = azureActiveContext$Token,
                                            `Content-type` = "application/json")),
              verbosity)

  if (status_code(r) == 204) {
    stop("Error: Storage Account not found")
  }
  if (status_code(r) == 409) {
    stop("Error: An operation for the storage account is in progress.")
  }
  if (status_code(r) != 200) stopWithAzureError(r)

  azureActiveContext$storageAccount <- storageAccount
  azureActiveContext$resourceGroup <- resourceGroup
  TRUE
}

