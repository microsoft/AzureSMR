GetSig <- function(AzureActiveContext, url, verb, key, StorageAccount,
                   Headers = NULL, Container = NULL, CMD = NULL, Size = NULL, ContentType = NULL,
                   DateS, verbose = FALSE) {

  if (length(Headers)){
    ARG1 <- paste0(Headers, "\nx-ms-date:", DateS, "\nx-ms-version:2015-04-05")
  } else {
    ARG1 <- paste0("x-ms-date:", DateS, "\nx-ms-version:2015-04-05")
  }

  ARG2 <- paste0("/", StorageAccount, "/", Container, CMD)

  SIG <- paste0(verb, "\n\n\n", Size, "\n\n", ContentType, "\n\n\n\n\n\n\n",
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
#' @inheritParams SetAzureContext
#' @inheritParams AzureAuthenticate
#' @inheritParams AzureSAGetKey

#' @family Storage account functions
#' @export
AzureListSA <- function(AzureActiveContext, ResourceGroup, SubscriptionID,
                        AzToken, verbose = FALSE) {
  if (missing(ResourceGroup)) {
    RGI <- AzureActiveContext$ResourceGroup
  } else {
    RGI = ResourceGroup
  }
  if (missing(AzToken)) {
    ATI <- AzureActiveContext$Token
  } else {
    ATI = AzToken
  }

  if (missing(SubscriptionID)) {
    SUBIDI <- AzureActiveContext$SubscriptionID
  } else {
    SUBIDI = SubscriptionID
  }
  if (!length(ATI)) {
    stop("Error: No Token / Not currently Authenticated.")
  }
  if (!length(SUBIDI)) {
    stop("Error: No SubscriptionID provided: Use SUBID argument or set in AzureContext")
  }
  verbosity <- if (verbose) httr::verbose(TRUE) else NULL
  SA <- if (missing(ResourceGroup)) {
    AzureListAllResources(AzureActiveContext,
                          Type = "Microsoft.Storage/storageAccounts")
  } else {
    AzureListAllResources(AzureActiveContext,
                          Type = "Microsoft.Storage/storageAccounts",
                          ResourceGroup = RGI)
  }
  rownames(SA) <- NULL
  SA$StorageAccount <- gsub(".*?/storageAccounts/(.*?)", "\\1", SA$ID)
  return(SA)
}



#' Get the Storage Keys for Specified Storage Account.
#'
#' @inheritParams SetAzureContext
#' @inheritParams AzureAuthenticate
#' @param StorageAccount StorageAccount
#' @param AzToken Token Object (or use AzureActiveContext)
#'
#' @family Storage account functions
#' @export
AzureSAGetKey <- function(AzureActiveContext, StorageAccount, AzToken,
                          ResourceGroup, SubscriptionID, verbose = FALSE) {
  AzureCheckToken(AzureActiveContext)

  if (missing(ResourceGroup)) {
    RGI <- AzureActiveContext$ResourceGroup
  } else {
    RGI = ResourceGroup
  }
  if (missing(AzToken)) {
    ATI <- AzureActiveContext$Token
  } else {
    ATI = AzToken
  }

  if (missing(SubscriptionID)) {
    SUBIDI <- AzureActiveContext$SubscriptionID
  } else (SUBIDI = SubscriptionID)
  if (!length(StorageAccount)) {
    stop("Error: No Valid StorageAccount provided")
  }
  if (length(RGI) < 1) {
    stop("Error: No ResourceGroup provided: Use ResourceGroup argument or set in AzureContext")
  }
  if (!length(ATI)) {
    stop("Error: No Token / Not currently Authenticated.")
  }
  if (!length(SUBIDI)) {
    stop("Error: No SubscriptionID provided: Use SUBID argument or set in AzureContext")
  }
  verbosity <- if (verbose)
    httr::verbose(TRUE) else NULL

  message("Fetching Storage Key..")

  URL <- paste("https://management.azure.com/subscriptions/", SUBIDI,
               "/resourceGroups/", RGI, "/providers/Microsoft.Storage/storageAccounts/",
               StorageAccount, "/listkeys?api-version=2016-01-01", sep = "")

  r <- POST(URL, add_headers(.headers = c(Host = "management.azure.com",
                                          Authorization = ATI,
                                          `Content-Type` = "application/json")),
            verbosity)


  rl <- content(r, "text", encoding = "UTF-8")
  df <- fromJSON(rl)
  AzureActiveContext$StorageAccount  <- StorageAccount
  AzureActiveContext$StorageAccountK <- StorageAccount
  AzureActiveContext$ResourceGroup   <- ResourceGroup
  AzureActiveContext$StorageKey      <- df$keys$value[1]
  return(AzureActiveContext$StorageKey)
}



#' Create an Azure Storage Account.
#'
#' @inheritParams SetAzureContext
#' @inheritParams AzureAuthenticate
#' @inheritParams AzureSAGetKey

#' @family Storage account functions
#' @export
AzureCreateStorageAccount <- function(AzureActiveContext, StorageAccount,
                                      AzToken, ResourceGroup, SubscriptionID, verbose = FALSE) {
  AzureCheckToken(AzureActiveContext)

  if (missing(ResourceGroup)) {
    RGI <- AzureActiveContext$ResourceGroup
  } else {
    RGI <- ResourceGroup
  }
  if (missing(AzToken)) {
    ATI <- AzureActiveContext$Token
  } else {
    ATI <- AzToken
  }

  if (missing(SubscriptionID)) {
    SUBIDI <- AzureActiveContext$SubscriptionID
  } else {
    SUBIDI <- SubscriptionID
  }
  if (!length(StorageAccount)) {
    stop("Error: No Valid StorageAccount provided")
  }
  if (length(RGI) < 1) {
    stop("Error: No ResourceGroup provided: Use ResourceGroup argument or set in AzureContext")
  }
  if (!length(ATI)) {
    stop("Error: No Token / Not currently Authenticated.")
  }
  if (!length(SUBIDI)) {
    stop("Error: No SubscriptionID provided: Use SUBID argument or set in AzureContext")
  }
  verbosity <- if (verbose)
    httr::verbose(TRUE) else NULL

  # https://management.azure.com/subscriptions/{subscriptionId}/resourceGroups/{resourceGroupName}/providers/Microsoft.Storage/storageAccounts/{accountName}?api-version={api-version}

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
               StorageAccount, "?api-version=2016-01-01", sep = "")

  r <- PUT(URL, add_headers(.headers = c(Host = "management.azure.com",
                                         Authorization = ATI,
                                         `Content-Type` = "application/json")),
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
  AzureActiveContext$StorageAccount <- StorageAccount
  AzureActiveContext$ResourceGroup  <- ResourceGroup
  return("Create request Accepted. It can take a few moments to provision the storage account")
}


#' Delete an Azure Storage Account.
#'
#' @inheritParams SetAzureContext
#' @inheritParams AzureAuthenticate
#' @inheritParams AzureSAGetKey

#' @family Storage account functions
#' @export
AzureDeleteStorageAccount <- function(AzureActiveContext, StorageAccount,
                                      AzToken, ResourceGroup, SubscriptionID, verbose = FALSE) {
  AzureCheckToken(AzureActiveContext)

  if (missing(ResourceGroup)) {
    RGI <- AzureActiveContext$ResourceGroup
  } else {
    RGI <- ResourceGroup
  }
  if (missing(AzToken)) {
    ATI <- AzureActiveContext$Token
  } else (ATI = AzToken)

  if (missing(SubscriptionID)) {
    SUBIDI <- AzureActiveContext$SubscriptionID
  } else {
    SUBIDI <- SubscriptionID
  }
  if (!length(StorageAccount)) {
    stop("Error: No Valid StorageAccount provided")
  }
  if (length(RGI) < 1) {
    stop("Error: No ResourceGroup provided: Use ResourceGroup argument or set in AzureContext")
  }
  if (!length(ATI)) {
    stop("Error: No Token / Not currently Authenticated.")
  }
  if (!length(SUBIDI)) {
    stop("Error: No SubscriptionID provided: Use SUBID argument or set in AzureContext")
  }
  verbosity <- if (verbose) httr::verbose(TRUE) else NULL


  URL <- paste("https://management.azure.com/subscriptions/", SUBIDI,
               "/resourceGroups/", RGI, "/providers/Microsoft.Storage/storageAccounts/",
               StorageAccount, "?api-version=2016-01-01", sep = "")

  r <- DELETE(URL, add_headers(.headers = c(Host = "management.azure.com",
                                            Authorization = ATI,
                                            `Content-Type` = "application/json")),
              verbosity)

  if (status_code(r) == 204) {
    stop(paste0("Error: Storage Account not found"))
  }
  if (status_code(r) == 409) {
    stop(paste0("Error: An operation for the storage account is in progress."))
  }
  if (status_code(r) != 200) stopWithAzureError(r)

  rl <- content(r, "text", encoding = "UTF-8")
  AzureActiveContext$StorageAccount <- StorageAccount
  AzureActiveContext$ResourceGroup <- ResourceGroup
  return("Done")
}

