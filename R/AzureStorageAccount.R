getSig <- function(azureActiveContext, url, verb, key, storageAccount,
                   headers = NULL, container = NULL, CMD = NULL, size = NULL, contenttype = NULL,
                   dateS, verbose = FALSE) {

  if (length(headers)){
    ARG1 <- paste0(headers, "\nx-ms-date:", dateS, "\nx-ms-version:2015-04-05")
  } else {
    ARG1 <- paste0("x-ms-date:", dateS, "\nx-ms-version:2015-04-05")
  }

  ARG2 <- paste0("/", storageAccount, "/", container, CMD)

  SIG <- paste0(verb, "\n\n\n", size, "\n\n", contenttype, "\n\n\n\n\n\n\n",
                ARG1, "\n", ARG2)
  if (verbose) message(paste0("TRACE: STRINGTOSIGN: ", SIG))
  base64encode(hmac(key = base64decode(key),
                    object = iconv(SIG, "ASCII",to = "UTF-8"),
                    algo = "sha256",
                    raw = TRUE)
  )

}

#' List Storage accounts.
#'
#' @inheritParams setAzureContext
#' @inheritParams azureAuthenticate
#' @inheritParams azureSAGetKey

#' @family Storage account functions
#' @export
azureListSA <- function(azureActiveContext, resourceGroup, subscriptionID,
                        azToken, verbose = FALSE) {
  if (missing(resourceGroup)) {
    RGI <- azureActiveContext$resourceGroup
  } else {
    RGI = resourceGroup
  }
  if (missing(azToken)) {
    ATI <- azureActiveContext$Token
  } else {
    ATI = azToken
  }

  if (missing(subscriptionID)) {
    SUBIDI <- azureActiveContext$subscriptionID
  } else {
    SUBIDI = subscriptionID
  }
  if (!length(ATI)) {
    stop("Error: No Token / Not currently Authenticated.")
  }
  if (!length(SUBIDI)) {
    stop("Error: No subscriptionID provided: Use SUBID argument or set in AzureContext")
  }
  verbosity <- if (verbose) httr::verbose(TRUE) else NULL
  SA <- if (missing(resourceGroup)) {
    azureListAllResources(azureActiveContext,
                          type = "Microsoft.Storage/storageAccounts")
  } else {
    azureListAllResources(azureActiveContext,
                          type = "Microsoft.Storage/storageAccounts",
                          resourceGroup = RGI)
  }
  rownames(SA) <- NULL
  SA$storageAccount <- gsub(".*?/storageAccounts/(.*?)", "\\1", SA$ID)
  return(SA)
}



#' Get the Storage Keys for Specified Storage Account.
#'
#' @inheritParams setAzureContext
#' @inheritParams azureAuthenticate
#' @param storageAccount storageAccount
#' @param azToken Token Object (or use azureActiveContext)
#'
#' @family Storage account functions
#' @export
azureSAGetKey <- function(azureActiveContext, storageAccount, azToken,
                          resourceGroup, subscriptionID, verbose = FALSE) {
  azureCheckToken(azureActiveContext)

  if (missing(resourceGroup)) {
    RGI <- azureActiveContext$resourceGroup
  } else {
    RGI = resourceGroup
  }
  if (missing(azToken)) {
    ATI <- azureActiveContext$Token
  } else {
    ATI = azToken
  }

  if (missing(subscriptionID)) {
    SUBIDI <- azureActiveContext$subscriptionID
  } else (SUBIDI = subscriptionID)
  if (!length(storageAccount)) {
    stop("Error: No Valid storageAccount provided")
  }
  if (length(RGI) < 1) {
    stop("Error: No resourceGroup provided: Use resourceGroup argument or set in AzureContext")
  }
  if (!length(ATI)) {
    stop("Error: No Token / Not currently Authenticated.")
  }
  if (!length(SUBIDI)) {
    stop("Error: No subscriptionID provided: Use SUBID argument or set in AzureContext")
  }
  verbosity <- if (verbose)
    httr::verbose(TRUE) else NULL

  message("Fetching Storage Key..")

  URL <- paste("https://management.azure.com/subscriptions/", SUBIDI,
               "/resourceGroups/", RGI, "/providers/Microsoft.Storage/storageAccounts/",
               storageAccount, "/listkeys?api-version=2016-01-01", sep = "")

  r <- POST(URL, add_headers(.headers = c(Host = "management.azure.com",
                                          Authorization = ATI,
                                          `Content-type` = "application/json")),
            verbosity)


  rl <- content(r, "text", encoding = "UTF-8")
  df <- fromJSON(rl)
  azureActiveContext$storageAccount  <- storageAccount
  azureActiveContext$storageAccountK <- storageAccount
  azureActiveContext$resourceGroup   <- resourceGroup
  azureActiveContext$storageKey      <- df$keys$value[1]
  return(azureActiveContext$storageKey)
}



#' Create an Azure Storage Account.
#'
#' @inheritParams setAzureContext
#' @inheritParams azureAuthenticate
#' @inheritParams azureSAGetKey

#' @family Storage account functions
#' @export
azureCreateStorageAccount <- function(azureActiveContext, storageAccount,
                                      azToken, resourceGroup, subscriptionID, verbose = FALSE) {
  azureCheckToken(azureActiveContext)

  if (missing(resourceGroup)) {
    RGI <- azureActiveContext$resourceGroup
  } else {
    RGI <- resourceGroup
  }
  if (missing(azToken)) {
    ATI <- azureActiveContext$Token
  } else {
    ATI <- azToken
  }

  if (missing(subscriptionID)) {
    SUBIDI <- azureActiveContext$subscriptionID
  } else {
    SUBIDI <- subscriptionID
  }
  if (!length(storageAccount)) {
    stop("Error: No Valid storageAccount provided")
  }
  if (length(RGI) < 1) {
    stop("Error: No resourceGroup provided: Use resourceGroup argument or set in AzureContext")
  }
  if (!length(ATI)) {
    stop("Error: No Token / Not currently Authenticated.")
  }
  if (!length(SUBIDI)) {
    stop("Error: No subscriptionID provided: Use SUBID argument or set in AzureContext")
  }
  verbosity <- if (verbose)
    httr::verbose(TRUE) else NULL

  # https://management.azure.com/subscriptions/{subscriptionId}/resourceGroups/{resourceGroupname}/providers/Microsoft.Storage/storageAccounts/{accountname}?api-version={api-version}

  bodyI = "
  {
  \"location\": \"northeurope\",
  \"tags\": {
  \"key1\": \"value1\"
  },
  \"properties\": {
  \"accessTier\": \"Hot\"
  },
  \"sku\": {
  \"name\": \"Standard_LRS\"
  },
  \"kind\": \"Storage\"
  }"

  bodyI <- "{
  \"location\": \"northeurope\",
  \"sku\": {
  \"name\": \"Standard_LRS\"
  }}"

  URL <- paste("https://management.azure.com/subscriptions/", SUBIDI,
               "/resourceGroups/", RGI, "/providers/Microsoft.Storage/storageAccounts/",
               storageAccount, "?api-version=2016-01-01", sep = "")

  r <- PUT(URL, add_headers(.headers = c(Host = "management.azure.com",
                                         Authorization = ATI,
                                         `Content-type` = "application/json")),
           body = bodyI,
           encode = "json", verbosity)

  if (status_code(r) == 409) {
    warning("409: Conflict : Account already exists with the same name")
    return(NULL)
  }


  if (status_code(r) == 200) {
    message("Account already exists with the same properties")
  } else if (status_code(r) != 202) {
    stopWithAzureError(r)
  }

  rl <- content(r, "text", encoding = "UTF-8")
  azureActiveContext$storageAccount <- storageAccount
  azureActiveContext$resourceGroup  <- resourceGroup
  return("Create request Accepted. It can take a few moments to provision the storage account")
}


#' Delete an Azure Storage Account.
#'
#' @inheritParams setAzureContext
#' @inheritParams azureAuthenticate
#' @inheritParams azureSAGetKey

#' @family Storage account functions
#' @export
azureDeletestorageAccount <- function(azureActiveContext, storageAccount,
                                      azToken, resourceGroup, subscriptionID, verbose = FALSE) {
  azureCheckToken(azureActiveContext)

  if (missing(resourceGroup)) {
    RGI <- azureActiveContext$resourceGroup
  } else {
    RGI <- resourceGroup
  }
  if (missing(azToken)) {
    ATI <- azureActiveContext$Token
  } else (ATI = azToken)

  if (missing(subscriptionID)) {
    SUBIDI <- azureActiveContext$subscriptionID
  } else {
    SUBIDI <- subscriptionID
  }
  if (!length(storageAccount)) {
    stop("Error: No Valid storageAccount provided")
  }
  if (length(RGI) < 1) {
    stop("Error: No resourceGroup provided: Use resourceGroup argument or set in AzureContext")
  }
  if (!length(ATI)) {
    stop("Error: No Token / Not currently Authenticated.")
  }
  if (!length(SUBIDI)) {
    stop("Error: No subscriptionID provided: Use SUBID argument or set in AzureContext")
  }
  verbosity <- if (verbose) httr::verbose(TRUE) else NULL


  URL <- paste("https://management.azure.com/subscriptions/", SUBIDI,
               "/resourceGroups/", RGI, "/providers/Microsoft.Storage/storageAccounts/",
               storageAccount, "?api-version=2016-01-01", sep = "")

  r <- DELETE(URL, add_headers(.headers = c(Host = "management.azure.com",
                                            Authorization = ATI,
                                            `Content-type` = "application/json")),
              verbosity)

  if (status_code(r) == 204) {
    stop(paste0("Error: Storage Account not found"))
  }
  if (status_code(r) == 409) {
    stop(paste0("Error: An operation for the storage account is in progress."))
  }
  if (status_code(r) != 200) stopWithAzureError(r)

  rl <- content(r, "text", encoding = "UTF-8")
  azureActiveContext$storageAccount <- storageAccount
  azureActiveContext$resourceGroup <- resourceGroup
  return("Done")
}

